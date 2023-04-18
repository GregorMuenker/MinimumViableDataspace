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

public class TransferMaLoSourceFactory implements DataSourceFactory {

    private BlobServiceClient srcBlobServiceClient;
    private final Monitor monitor;

    TransferMaLoSourceFactory(Monitor monitor, BlobServiceClient srcBlobServiceClient) {
        this.monitor = monitor;
        this.srcBlobServiceClient = srcBlobServiceClient;
    }

    @Override
    public boolean canHandle(DataFlowRequest dataRequest) {
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
        return Result.success(true);
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var malo = getMaLoInfo(request);
        //TODO add and remove info for lieferant
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
