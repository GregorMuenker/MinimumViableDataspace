/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.makochain;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

public class TransferDataSinkFactory implements DataSinkFactory {
    private final Monitor monitor;
    private final ExecutorService executorService;
    private final int partitionSize;
    private BlobServiceClient destBlobServiceClient;

    TransferDataSinkFactory(Monitor monitor, ExecutorService executorService, int partitionSize, BlobServiceClient destBlobServiceClient) {
        this.monitor = monitor;
        this.executorService = executorService;
        this.partitionSize = partitionSize;
        this.destBlobServiceClient = destBlobServiceClient;
        monitor.info("RequestNewProvider Extension Sink Factory");
    }

    @Override
    public boolean canHandle(DataFlowRequest dataRequest) {
        monitor.info("RequestNewProvider Extension Sink Factory canhandle " + dataRequest.getSourceDataAddress().getType());
        return "AzureStorage".equalsIgnoreCase(dataRequest.getSourceDataAddress().getType());
    }

    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        return Result.success(true);
    }

    @Override
    public DataSink createSink(DataFlowRequest request) {
        var destination = request.getDestinationDataAddress();

        var blobname = destination.getProperty("blobname");
        var containerName = destination.getProperty("container");

        if (blobname == null) {
            blobname = "Copy";
        }
        monitor.info("RequestNewProvider Extension Sink " + containerName + " - " + blobname);

        BlobClient destBlob = destBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);

        return TransferDataSink.Builder.newInstance()
                .blob(destBlob)
                .name(blobname)
                .monitor(monitor)
                .requestId(request.getId())
                .partitionSize(partitionSize)
                .executorService(executorService)
                .monitor(monitor)
                .build();
    }
}
