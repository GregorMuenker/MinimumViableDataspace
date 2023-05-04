/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.malolieferant;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

import static java.lang.String.format;

public class LfMaLoDataSinkFactory implements DataSinkFactory {
    private final Monitor monitor;
    private final ExecutorService executorService;
    private final int partitionSize;

    LfMaLoDataSinkFactory(Monitor monitor, ExecutorService executorService, int partitionSize) {
        this.monitor = monitor;
        this.executorService = executorService;
        this.partitionSize = partitionSize;
    }

    @Override
    public boolean canHandle(DataFlowRequest dataRequest) {
        return "MaLo_lfr".equalsIgnoreCase(dataRequest.getSourceDataAddress().getType());
    }

    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        var destination = request.getDestinationDataAddress();
        var blobname = destination.getProperty("blobname");
        var containerName = destination.getProperty("container");
        var sasToken = destination.getProperty("sasToken");
        var blobAccount = destination.getProperty("account");
        if (blobname != null && containerName != null && sasToken != null && blobAccount != null) {
            return Result.success(true);
        }
        return Result.failure("DataDestination has missing Attributes");
    }

    @Override
    public DataSink createSink(DataFlowRequest request) {
        var destination = request.getDestinationDataAddress();
        var blobname = destination.getProperty("blobname");
        var containerName = destination.getProperty("container");
        var sasToken = destination.getProperty("sasToken");
        var blobAccount = destination.getProperty("account");

        BlobClient destBlob = new BlobClientBuilder()
                .endpoint(format("http://azurite:10000/%s", blobAccount))
                .sasToken(sasToken)
                .containerName(containerName)
                .blobName(blobname)
                .buildClient();

        return LfMaLoDataSink.Builder.newInstance()
                .blob(destBlob)
                .name(blobname)
                .requestId(request.getId())
                .partitionSize(partitionSize)
                .executorService(executorService)
                .monitor(monitor)
                .build();
    }
}
