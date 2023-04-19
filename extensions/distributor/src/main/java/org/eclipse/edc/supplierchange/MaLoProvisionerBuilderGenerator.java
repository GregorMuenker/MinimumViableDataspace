/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import org.eclipse.edc.connector.transfer.spi.provision.ProviderResourceDefinitionGenerator;
import org.eclipse.edc.connector.transfer.spi.types.DataRequest;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.jetbrains.annotations.Nullable;
import org.json.JSONObject;

import java.util.Objects;

import static java.util.UUID.randomUUID;

public class MaLoProvisionerBuilderGenerator implements ProviderResourceDefinitionGenerator {

    private BlobServiceClient srcBlobServiceClient;
    private final Monitor monitor;

    public MaLoProvisionerBuilderGenerator(BlobServiceClient srcBlobServiceClient, Monitor monitor) {
        this.srcBlobServiceClient = srcBlobServiceClient;
        this.monitor = monitor;
    }

    @Override
    public @Nullable ResourceDefinition generate(DataRequest dataRequest, DataAddress assetAddress, Policy policy) {
        var blobname = assetAddress.getProperty("blobname");
        var containerName = assetAddress.getProperty("container");

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);
        if (!srcBlob.exists()) {
            //should exist as validated in SourceFactory
            return null;
        }
        String maLo = srcBlob.downloadContent().toString();

        BlobContainerClient destTempClient = srcBlobServiceClient.createBlobContainerIfNotExists("temp-container");

        return MaLoResourceDefinition.Builder.newInstance()
                .id(randomUUID().toString())
                .maLo(new JSONObject(maLo))
                .requestedStartDate(dataRequest.getProperties().get("start_date"))
                .requestedEndDate(dataRequest.getProperties().get("end_date"))
                .tempContainer(destTempClient)
                .build();
    }

    @Override
    public boolean canGenerate(DataRequest dataRequest, DataAddress assetAddress, Policy policy) {
        Objects.requireNonNull(dataRequest, "dataRequest must always be provided");
        Objects.requireNonNull(assetAddress, "assetAddress must always be provided");
        Objects.requireNonNull(policy, "policy must always be provided");
        return "MaLo".equalsIgnoreCase(assetAddress.getType());
    }
    
}
