/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import java.util.concurrent.CompletableFuture;

import org.eclipse.edc.connector.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;

public class MaLoProvisioner implements Provisioner<MaLoResourceDefinition,MaLoProvisionedResource>{

    private final Monitor monitor;

    public MaLoProvisioner(Monitor monitor) {
        this.monitor = monitor;
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
        String maloId = resourceDefinition.getmaloId();

        monitor.debug("MaLo is being provisioned: " + maloId);

        if (request.failed()) {
            return completedFuture(StatusResult.failure(ResponseStatus.FATAL_ERROR, request.getFailureDetail()));
        }

        return with(retryPolicy).getAsync(() -> blobStoreApi.exists(accountName, containerName))
                .thenCompose(exists -> {
                    if (exists) {
                        return reusingExistingContainer(containerName);
                    } else {
                        return createContainer(containerName, accountName);
                    }
                })
                .thenCompose(empty -> createContainerSasToken(containerName, accountName, expiryTime))
                .thenApply(writeOnlySas -> {
                    // Ensure resource name is unique to avoid key collisions in local and remote vaults
                    String resourceName = resourceDefinition.getId() + "-container";
                    var resource = ObjectContainerProvisionedResource.Builder.newInstance()
                            .id(containerName)
                            .accountName(accountName)
                            .containerName(containerName)
                            .resourceDefinitionId(resourceDefinition.getId())
                            .transferProcessId(resourceDefinition.getTransferProcessId())
                            .resourceName(resourceName)
                            .hasToken(true)
                            .build();

                    var secretToken = new AzureSasToken("?" + writeOnlySas, expiryTime.toInstant().toEpochMilli());

                    var response = ProvisionResponse.Builder.newInstance().resource(resource).secretToken(secretToken).build();
                    return StatusResult.success(response);
                });
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(MaLoProvisionedResource provisionedResource, Policy policy) {
        return with(retryPolicy).runAsync(() -> blobStoreApi.deleteContainer(provisionedResource.getAccountName(), provisionedResource.getContainerName()))
                //the sas token will expire automatically. there is no way of revoking them other than a stored access policy
                .thenApply(empty -> StatusResult.success(DeprovisionedResource.Builder.newInstance().provisionedResourceId(provisionedResource.getId()).build()));
    }
    
}
