/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import org.eclipse.edc.connector.spi.transferprocess.TransferProcessService;
import org.eclipse.edc.connector.transfer.spi.TransferProcessManager;
import org.eclipse.edc.connector.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.transfer.spi.types.DataRequest;
import org.eclipse.edc.connector.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class MaLoProvisioner implements Provisioner<MaLoResourceDefinition, MaLoProvisionedResource> {

    private final Monitor monitor;
    private final TransferProcessManager processManager;
    private final TransferProcessService transferProcessService;

    public MaLoProvisioner(Monitor monitor, TransferProcessManager processManager, TransferProcessService transferProcessService) {
        this.monitor = monitor;
        this.processManager = processManager;
        this.transferProcessService = transferProcessService;
    }

    @Override
    public boolean canProvision(ResourceDefinition resourceDefinition) {
        return resourceDefinition instanceof MaLoResourceDefinition;
    }

    @Override
    public boolean canDeprovision(ProvisionedResource resourceDefinition) {
        return resourceDefinition instanceof MaLoProvisionedResource;
    }

    @Override
    public CompletableFuture<StatusResult<ProvisionResponse>> provision(MaLoResourceDefinition resourceDefinition, Policy policy) {
        
        return CompletableFuture.supplyAsync(() -> {
            JSONObject maLo = resourceDefinition.getmaLo();
    
            JSONObject lieferantAlt = maLo.getJSONObject("lieferant");
            JSONArray belieferungen = maLo.getJSONArray("belieferungen");

            LocalDate requestedDate = LocalDate.parse(resourceDefinition.getRequestDate());
            LocalDate now = LocalDate.now();
            LocalDate lastDelivery = now;

            //TODO get Last supplied Date
            for (int i = 0; i < belieferungen.length(); i++) {
                JSONObject jsonObject = belieferungen.getJSONObject(i);
                LocalDate endDate = LocalDate.parse(jsonObject.getString("bis"));
                if (endDate.isAfter(lastDelivery)) {
                    lastDelivery = endDate;
                }
            }
    
            monitor.info("MaLo is supplied until " + lastDelivery.toString());
    
            if (lastDelivery.isEqual(now)) {
                // kein aktueller Lieferant
                monitor.info("Register MaLo Extension Source Factory no supplier");
                var resource = MaLoProvisionedResource.Builder.newInstance()
                        .maLo(maLo)
                        .id("" + maLo.hashCode())
                        .resourceDefinitionId(resourceDefinition.getId())
                        .transferProcessId(resourceDefinition.getTransferProcessId())
                        .build();
                return StatusResult.success(ProvisionResponse.Builder.newInstance().resource(resource).build());
            } else if (lieferantAlt != null) {
                //TODO convert to Date and check if fits
    
                monitor.info("Register MaLo Extension Source Factory supplier");
    
                //prepare Request to old Supplier
                String assetId = "MaLo_" + maLo.optString("maLo") + "_" + lieferantAlt.optString("name");
    
                OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
                BlobContainerSasPermission permission = new BlobContainerSasPermission()
                        .setWritePermission(true)
                        .setAddPermission(true)
                        .setCreatePermission(true);
                BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiryTime, permission)
                        .setStartTime(OffsetDateTime.now());
    
                String sas = resourceDefinition.getTempContainer().generateSas(values);

                Map<String, String> additionalInfo = new HashMap<>();
                additionalInfo.put("end_date", resourceDefinition.getRequestDate());
    
                var dataRequest = DataRequest.Builder.newInstance()
                        .id(UUID.randomUUID().toString()) // this is not relevant, thus can be random
                        .connectorAddress(lieferantAlt.getString("connector"))
                        .protocol("ids-multipart")
                        .connectorId("consumer")
                        .assetId(assetId)
                        .dataDestination(DataAddress.Builder.newInstance()
                                .type("MaLo_lfr")
                                .property("blobname", assetId + "_temp")
                                .property("request", "change")
                                .property("sasToken", sas)
                                .property("container", resourceDefinition.getTempContainer().getBlobContainerName())
                                .property("account", resourceDefinition.getTempContainer().getAccountName())
                                .property("date", lastDelivery.toString())
                                .build()) 
                        .managedResources(false)
                        .properties(additionalInfo)
                        .contractId(lieferantAlt.getString("dataContract"))
                        .build();
                /* var transferRequest = TransferRequest.Builder.newInstance()
                        .dataRequest(dataRequest)
                        .build();
                */
    
                var result = processManager.initiateConsumerRequest(dataRequest);
                monitor.info("Register MaLo Extension Source Factory sent request");
                
                try {
                    monitor.info("MaLo Extension sleep");
                    Thread.sleep(5000); // wait for transfer
                    monitor.info("MaLo Extension wakeup");
                } catch (Exception e) {
                    // TODO: handle exception
                }

                /* 
                QuerySpec spec = QuerySpec.Builder.newInstance().filter("type=CONSUMER").build();
    
                try (var stream = transferProcessService.query(spec).orElseThrow(exceptionMapper(TransferProcess.class, null))) {
                    
                    monitor.info("MaLo Extension seek transfer " + result.getContent());
                    if (stream.count() > 1) {
                        StatusResult.failure(ResponseStatus.FATAL_ERROR, "more than one Request");
                    }
                    var transferProcess = stream.skip(1).findFirst().;
                    transferProcess.getDataRequest();
                    monitor.info("MaLo Extension : " + transferProcess.getDataRequest().getAssetId());
    
                }
                */

                for (BlobItem blobItem : resourceDefinition.getTempContainer().listBlobs()) {
                    if (blobItem.getName().equalsIgnoreCase(assetId + "_temp")) {
                        BlobClient lieferantAltMaLo = resourceDefinition.getTempContainer().getBlobClient(blobItem.getName());

                        // combine the info

                        resourceDefinition.getTempContainer().delete();

                        var resource = MaLoProvisionedResource.Builder.newInstance()
                                .maLo(maLo)
                                .id("" + maLo.hashCode())
                                .resourceDefinitionId(resourceDefinition.getId())
                                .transferProcessId(resourceDefinition.getTransferProcessId())
                                .build();

                        var response = ProvisionResponse.Builder.newInstance().resource(resource).build();
                        return StatusResult.success(response);
                    }
                    
                }
            }
            resourceDefinition.getTempContainer().delete();
            transferProcessService.cancel(resourceDefinition.getTransferProcessId());
            return StatusResult.failure(ResponseStatus.FATAL_ERROR, "no evaluation");
        }); 
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(MaLoProvisionedResource provisionedResource, Policy policy) {
        var deprovisionedResource = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId(provisionedResource.getTransferProcessId())
                .build();
        return completedFuture(StatusResult.success(deprovisionedResource));
    }
    
}
