/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

import static java.lang.String.format;

public class TransferMaLoSinkFactory implements DataSinkFactory {
    private final Monitor monitor;
    private final ExecutorService executorService;
    private final int partitionSize;

    TransferMaLoSinkFactory(Monitor monitor, ExecutorService executorService, int partitionSize) {
        this.monitor = monitor;
        this.executorService = executorService;
        this.partitionSize = partitionSize;
        monitor.info("Register MaLo Sink Factory");
    }

    @Override
    public boolean canHandle(DataFlowRequest dataRequest) {
        monitor.info("Register MaLo Extension Sink Factory canhandle " + dataRequest.getSourceDataAddress().getType());
        return "MaLo".equalsIgnoreCase(dataRequest.getSourceDataAddress().getType());
    }

    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        monitor.info("Register MaLo Extension Sink Factory validate");
        return Result.success(true);
    }

    @Override
    public DataSink createSink(DataFlowRequest request) {
        var destination = request.getDestinationDataAddress();
        var blobname = destination.getProperty("blobname");
        var containerName = destination.getProperty("container");
        var sasToken = destination.getProperty("sasToken");
        var blobAccount = destination.getProperty("account");

        if (blobname == null) {
            blobname = "Transferred MaLo Reqest Id: " + request.getId();
        }
        
        //Create Destination Blob with sas Token
        BlobContainerClient destContainer = new BlobContainerClientBuilder()
                .endpoint(format("http://azurite:10000/%s", blobAccount) + "/" + containerName + sasToken)
                .buildClient();
        BlobClient destBlob = destContainer.getBlobClient(blobname);

        return TransferMaLoSink.Builder.newInstance()
                .blob(destBlob)
                .name(blobname)
                .requestId(request.getId())
                .partitionSize(partitionSize)
                .executorService(executorService)
                .monitor(monitor)
                .build();
    }
}
