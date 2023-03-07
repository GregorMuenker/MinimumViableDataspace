/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.makochain;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

public class TransferDataSourceFactory implements DataSourceFactory {

    private BlobServiceClient srcBlobServiceClient;
    private final Monitor monitor;

    TransferDataSourceFactory(Monitor monitor, BlobServiceClient srcBlobServiceClient) {
        this.monitor = monitor;
        this.srcBlobServiceClient = srcBlobServiceClient;
        monitor.info("RequestNewProvider Extension Source Factory");
    }

    @Override
    public boolean canHandle(DataFlowRequest dataRequest) {
        monitor.info("RequestNewProvider Extension Source Factory canhandle" + dataRequest.getSourceDataAddress().getType());
        return "AzureStorage".equalsIgnoreCase(dataRequest.getSourceDataAddress().getType());
    }

    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        // verify source path
        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");
        
        monitor.info("RequestNewProvider Extension Source " + blobname + " : "  + containerName);

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);

        monitor.info("RequestNewProvider Extension Source " +  srcBlob.getBlobName() + " ! " + srcBlob.getAccountName());
        monitor.info("RequestNewProvider Extension Source " +  srcBlob.exists() + " ? " + srcBlob.getBlobUrl());
        /* 
        if (!srcBlob.exists()) {
            return Result.failure("Source " + srcBlob.getBlobName() + " does not exist!");
        }
        */
        return Result.success(true);
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var source = getJson(request);
        monitor.info("RequestNewProvider Extension Source " + source.toString());
        return new TransferDataSource(monitor, source, request.getSourceDataAddress().getProperty("blobname"));
    }

    @NotNull
    private JSONObject getJson(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        // verify source path
        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");

        monitor.info("RequestNewProvider Extension Source Json " + blobname + " : "  + containerName);

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);
        
        monitor.info("RequestNewProvider Extension Source Json" +  srcBlob.getBlobName() + " !");

        String s = srcBlob.downloadContent().toString();

        monitor.info("RequestNewProvider Extension Source blob " + s);

        return new JSONObject(s);
    }

}
