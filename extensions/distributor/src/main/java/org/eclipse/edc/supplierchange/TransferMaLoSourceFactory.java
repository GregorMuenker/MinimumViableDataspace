/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
//import org.eclipse.edc.api.transformer.DtoTransformerRegistry;
//import org.eclipse.edc.connector.api.management.transferprocess.model.TransferProcessDto;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.connector.spi.transferprocess.TransferProcessService;
import org.eclipse.edc.connector.transfer.spi.TransferProcessManager;
import org.eclipse.edc.connector.transfer.spi.types.DataRequest;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.eclipse.edc.web.spi.exception.ServiceResultHandler.exceptionMapper;

public class TransferMaLoSourceFactory implements DataSourceFactory {

    private BlobServiceClient srcBlobServiceClient;
    private final Monitor monitor;
    private final TransferProcessManager processManager;
    private final TransferProcessService transferProcessService;
    //private final DtoTransformerRegistry transformerRegistry;

    TransferMaLoSourceFactory(Monitor monitor, BlobServiceClient srcBlobServiceClient,
            TransferProcessManager processManager, TransferProcessService transferProcessService/* , DtoTransformerRegistry transformerRegistry*/) {
        this.monitor = monitor;
        this.srcBlobServiceClient = srcBlobServiceClient;
        this.processManager = processManager;
        this.transferProcessService = transferProcessService;
        //this.transformerRegistry = transformerRegistry;
        monitor.info("Register MaLo Extension Source Factory");
    }

    @Override
    public boolean canHandle(DataFlowRequest dataRequest) {
        monitor.info("Register MaLo Extension Source Factory canhandle" + dataRequest.getSourceDataAddress().getType());
        return "MaLo".equalsIgnoreCase(dataRequest.getSourceDataAddress().getType());
    }

    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();

        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");
        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);

        if (!srcBlob.exists()) {
            return Result.failure("Source " + srcBlob.getBlobName() + " does not exist!");
        }

        String malo = getMaLoInfo(request);
        monitor.info("Register MaLo Extension Source Factory " + malo);

        JSONObject jsonMalo = new JSONObject(malo);

        String beliefertBis = jsonMalo.optString("beliefert_bis");
        JSONObject lieferantAlt = jsonMalo.optJSONObject("lieferant");

        monitor.info("Register MaLo Extension Source Factory " + beliefertBis);

        if (beliefertBis == "" || beliefertBis == null) {
            // kein aktueller Lieferant
            monitor.info("Register MaLo Extension Source Factory no supplier");
            return Result.success(true);
        } else if (lieferantAlt != null) {
            // convert to Date and check if fits
            var datum = dataAddress.getProperty("datum");

            monitor.info("Register MaLo Extension Source Factory supplier");

            // anfrage an liefeant alt
            String lieferantAltConn = lieferantAlt.optString("connector");
            String assedId = "MaLo_" + jsonMalo.optString("maLo") + "_" + lieferantAlt.optString("name");
            String contractId = lieferantAlt.optString("dataContract");

            OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
            BlobContainerSasPermission permission = new BlobContainerSasPermission()
                    .setWritePermission(true)
                    .setAddPermission(true)
                    .setCreatePermission(true);

            BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiryTime, permission)
                    .setStartTime(OffsetDateTime.now());

            String sas = srcBlobServiceClient.getBlobContainerClient(containerName).generateSas(values);

            monitor.info(
                    "Register MaLo Extension Source Factory " + lieferantAltConn + "|" + assedId + "|" + contractId);

            Map<String, String> additionalInfo = new HashMap<>();
            additionalInfo.put("end_date", datum);

            var dataRequest = DataRequest.Builder.newInstance()
                    .id(UUID.randomUUID().toString()) // this is not relevant, thus can be random
                    .connectorAddress(lieferantAltConn) // the address of the provider connector
                    .protocol("ids-multipart")
                    .connectorId("consumer")
                    .assetId(assedId)
                    .dataDestination(DataAddress.Builder.newInstance()
                            .type("MaLo_lfr")
                            .property("blobname", assedId.replace("MaLo_", "temp_"))
                            .property("request", "change")
                            .property("sasToken", sas)
                            .property("container", containerName)
                            .property("blobname", assedId)
                            .property("account", srcBlobServiceClient.getAccountName())
                            .property("date", beliefertBis)
                            .build()) 
                    .managedResources(false) // we do not need any provisioning
                    //.properties(additionalInfo)
                    .contractId(contractId)
                    .build();
            /*
             * .transferType(object.getTransferType())
             * .destinationType(object.getDataDestination().getType())
             * .properties(object.getProperties())
             * 
             * DataRequest.Builder.newInstance()
             * .id(id)
             * .assetId(object.getAssetId())
             * .connectorId(object.getConnectorId())
             * .dataDestination(object.getDataDestination())
             * .connectorAddress(object.getConnectorAddress())
             * .contractId(object.getContractId())
             * .transferType(object.getTransferType())
             * .destinationType(object.getDataDestination().getType())
             * .properties(object.getProperties())
             * .managedResources(object.isManagedResources())
             * .protocol(object.getProtocol())
             * .dataDestination(object.getDataDestination())
             * .build();
             */
            /* var transferRequest = TransferRequest.Builder.newInstance()
                    .dataRequest(dataRequest)
                    .build();
            */

            var result = processManager.initiateConsumerRequest(dataRequest);
            monitor.info("Register MaLo Extension Source Factory sent request");
            
            try {
                Thread.sleep(10000); // wait for transfer
            } catch (Exception e) {
                // TODO: handle exception
            }

            QuerySpec spec = QuerySpec.max();

            try (var stream = transferProcessService.query(spec).orElseThrow(exceptionMapper(TransferProcess.class, null))) {
                if (stream.count() > 1) {
                    return Result.failure("more than one request");
                }
                var transferProcess = stream.findFirst().get();
                transferProcess.getDataRequest();

            }
            monitor.info("Register MaLo Extension Source Factory " + result.getContent());
            return Result.success(!result.failed());
        }

        monitor.info("Register MaLo Extension Source validate true " + srcBlob.getBlobName());
        return Result.success(true);
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var malo = getMaLoInfo(request);
        // add and remove info for lieferant
        return new TransferMaLoSource(malo, request.getSourceDataAddress().getProperty("blobname"));
    }

    @NotNull
    private String getMaLoInfo(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);

        return srcBlob.downloadContent().toString();
    }

}
