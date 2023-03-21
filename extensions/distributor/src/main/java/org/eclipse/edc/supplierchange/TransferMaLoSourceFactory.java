/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

public class TransferMaLoSourceFactory implements DataSourceFactory {

    private BlobServiceClient srcBlobServiceClient;
    private final Monitor monitor;

    TransferMaLoSourceFactory(Monitor monitor, BlobServiceClient srcBlobServiceClient) {
        this.monitor = monitor;
        this.srcBlobServiceClient = srcBlobServiceClient;
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

        JSONObject jsonMalo = new JSONObject(malo);

        String beliefertBis = jsonMalo.optString("beliefert_bis");

        if (beliefertBis == "" || beliefertBis == null) {
            //kein aktueller Lieferant
        } else {
            //convert to Date and check if fits
            var datum = dataAddress.getProperty("datum"); 
            //anfrage an liefeant alt
            /* 
        Objects.requireNonNull(filename, "filename");
        Objects.requireNonNull(connectorAddress, "connectorAddress");

        var dataRequest = DataRequest.Builder.newInstance()
                .id(UUID.randomUUID().toString()) //this is not relevant, thus can be random
                .connectorAddress(connectorAddress) //the address of the provider connector
                .protocol("ids-multipart")
                .connectorId("consumer")
                .assetId(filename)
                .dataDestination(DataAddress.Builder.newInstance()
                        .type("File") //the provider uses this to select the correct DataFlowController
                        .property("path", destinationPath) //where we want the file to be stored
                        .build())
                .managedResources(false) //we do not need any provisioning
                .contractId(contractId)
                .build();

        var result = processManager.initiateConsumerRequest(dataRequest);
        */
            String lieferantAlt = jsonMalo.optString("lieferant");
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
