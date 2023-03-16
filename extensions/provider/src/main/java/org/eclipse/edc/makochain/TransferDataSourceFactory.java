/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.makochain;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import org.eclipse.edc.api.transformer.DtoTransformerRegistry;
//import org.eclipse.edc.connector.api.management.transferprocess.model.TerminateTransferDto;
import org.eclipse.edc.connector.api.management.transferprocess.model.TransferProcessDto;
import org.eclipse.edc.connector.api.management.transferprocess.model.TransferRequestDto;
import org.eclipse.edc.connector.api.management.transferprocess.model.TransferState;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.connector.spi.transferprocess.TransferProcessService;
import org.eclipse.edc.connector.transfer.spi.types.DataRequest;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
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
        return "MaLo".equalsIgnoreCase(dataRequest.getSourceDataAddress().getType());
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
        
        monitor.info("RequestNewProvider Extension Source validate true "+ srcBlob.getBlobName());
        return Result.success(true);
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var malo = getMaLoInfo(request);

        TransferProcessService service = new TransferProcessService(null, null);
        TransferRequestDto transferRequest = new TransferRequestDto(null, null);
        DtoTransformerRegistry transformerRegistry;
        var transformResult = transformerRegistry.transform(transferRequest, DataRequest.class);
        if (transformResult.failed()) {
            return null;
        }
        monitor.debug("Starting transfer for asset " + transferRequest.getAssetId());

        var dataRequest = transformResult.getContent();
        var result = service.initiateTransfer(dataRequest); // .orElseThrow(exceptionMapper(TransferProcess.class, transferRequest.getId()));

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