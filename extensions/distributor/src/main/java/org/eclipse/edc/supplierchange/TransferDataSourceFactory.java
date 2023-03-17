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

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);

        //ToDo validate if can be changed until requested
        //srcBlob.downloadContent().toString() 

        if (!srcBlob.exists()) {
            return Result.failure("Source " + srcBlob.getBlobName() + " does not exist!");
        }
        
        monitor.info("RequestNewProvider Extension Source validate true " + srcBlob.getBlobName());
        return Result.success(true);
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var malo = getMaLoInfo(request);
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
        return new TransferDataSource(monitor, malo, request.getSourceDataAddress().getProperty("blobname"));
    }

    @NotNull
    private String getMaLoInfo(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        // verify source path
        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");

        monitor.info("RequestNewProvider Extension Source Json " + blobname + " : "  + containerName);

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);
        
        monitor.info("RequestNewProvider Extension Source Json" +  srcBlob.getBlobName() + " !");

        return srcBlob.downloadContent().toString();
    }

}
