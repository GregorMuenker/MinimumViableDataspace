/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.malonetzbetr;

import com.azure.core.util.BinaryData;
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
            BlobClient maloBlob = resourceDefinition.getMaloBlob();
            JSONObject maloJson = new JSONObject(maloBlob.downloadContent().toString());
            JSONArray belieferungen = maloJson.getJSONArray("belieferungen");

            LocalDate requestedStartDate = LocalDate.parse(resourceDefinition.getRequestedStartDate());
            LocalDate requestedEndDate = LocalDate.parse(resourceDefinition.getRequestedEndDate());

            JSONArray conflicts = new JSONArray();

            //check if there are suppliers in this Timeframe
            for (int i = 0; i < belieferungen.length(); i++) {
                JSONObject jsonObject = belieferungen.getJSONObject(i);
                LocalDate endDate = LocalDate.parse(jsonObject.getString("bis"));
                if (requestedStartDate.isBefore(endDate)) {
                    conflicts.put(jsonObject);
                }
            }
    
            if (conflicts.length() == 0) {
                // kein Konflikt mit den Belieferungen 
                monitor.info("No Conflicts");
                var resource = MaLoProvisionedResource.Builder.newInstance()
                        .maLo(maloJson)
                        .id("" + maloJson.hashCode())
                        .resourceDefinitionId(resourceDefinition.getId())
                        .transferProcessId(resourceDefinition.getTransferProcessId())
                        .build();
                resourceDefinition.getTempContainer().delete();
                return StatusResult.success(ProvisionResponse.Builder.newInstance().resource(resource).build());
            } else {
                for (int i = 0; i < conflicts.length(); i++) {
                    JSONObject conflictLieferant = conflicts.getJSONObject(i).getJSONObject("lieferant");
                    monitor.info("Request end of contrat of old supplier " + conflictLieferant.getString("name"));
                    
                    //prepare Request to old Supplier
                    String assetId = "MaLo_" + maloJson.getString("maLo") + "_" + conflictLieferant.getString("name");

                    OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
                    BlobContainerSasPermission permission = new BlobContainerSasPermission()
                            .setWritePermission(true)
                            .setAddPermission(true)
                            .setCreatePermission(true);
                    BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiryTime, permission)
                            .setStartTime(OffsetDateTime.now());
    
                    String sas = resourceDefinition.getTempContainer().generateSas(values);

                    Map<String, String> additionalInfo = new HashMap<>();
                    additionalInfo.put("date", resourceDefinition.getRequestedStartDate());
                    additionalInfo.put("request", "change");
    
                    var dataRequest = DataRequest.Builder.newInstance()
                            .id(UUID.randomUUID().toString()) // this is not relevant, thus can be random
                            .connectorAddress(conflictLieferant.getString("connector"))
                            .protocol("ids-multipart")
                            .connectorId("consumer")
                            .assetId(assetId)
                            .dataDestination(DataAddress.Builder.newInstance()
                                    .type("MaLo_lfr")
                                    .property("blobname", assetId + "_temp" + i)
                                    .property("sasToken", sas)
                                    .property("container", resourceDefinition.getTempContainer().getBlobContainerName())
                                    .property("account", resourceDefinition.getTempContainer().getAccountName())
                                    .build()) 
                            .managedResources(false)
                            .properties(additionalInfo)
                            .contractId(conflictLieferant.getString("dataContract"))
                            .build();
                    /* var transferRequest = TransferRequest.Builder.newInstance()
                        .dataRequest(dataRequest)
                        .build();
                    */
    
                    var result = processManager.initiateConsumerRequest(dataRequest);
                    monitor.info("sent request " + result.getContent());
                }
                    
                //TODO: change to Events
                try {
                    monitor.info("MaLo Extension sleep");
                    Thread.sleep(5000); // wait for transfer
                    monitor.info("MaLo Extension wakeup");
                } catch (Exception e) {
                    // ToDo: handle exception
                }

                int changedContracts = 0;
                for (BlobItem blobItem : resourceDefinition.getTempContainer().listBlobs()) {
                    for (int i = 0; i < conflicts.length(); i++) {
                        if (blobItem.getName().equalsIgnoreCase("MaLo_" + maloJson.getString("maLo") + "_" + conflicts.getJSONObject(i).getJSONObject("lieferant").getString("name") + "_temp" + i)) {
                            changedContracts++;

                            //log changes in Malo json
                            BlobClient lieferantAltResponse = resourceDefinition.getTempContainer().getBlobClient(blobItem.getName());
                            JSONObject lfResponse = new JSONObject(lieferantAltResponse.downloadContent().toString());
                            var conflict = conflicts.getJSONObject(i).put("bis", lfResponse.getString("end_date"));
                            for (int e = 0; e < belieferungen.length(); e++) {
                                if (belieferungen.getJSONObject(e).getString("von") == conflict.getString("von")) {
                                    belieferungen.put(e, conflict);
                                }
                            }
                            break;
                        }
                    }
                }
                //update Malo with changes
                maloBlob.upload(BinaryData.fromString(maloJson.put("belieferungen", belieferungen).toString(4)), true);

                resourceDefinition.getTempContainer().delete();

                if (changedContracts == conflicts.length()) {

                    var resource = MaLoProvisionedResource.Builder.newInstance()
                            .maLo(maloJson)
                            .id("" + maloJson.hashCode())
                            .resourceDefinitionId(resourceDefinition.getId())
                            .transferProcessId(resourceDefinition.getTransferProcessId())
                            .build();

                    var response = ProvisionResponse.Builder.newInstance().resource(resource).build();
                    return StatusResult.success(response);
                }
                return StatusResult.failure(ResponseStatus.FATAL_ERROR, "Lieferant alt Failed to respond");
            }
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
