/*
 * Gregor Münker
 */

package org.eclipse.edc.makochain;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

public class TransferDataSourceFactory implements DataSourceFactory {

    private BlobServiceClient srcBlobServiceClient;

    TransferDataSourceFactory(BlobServiceClient srcBlobServiceClient) {
        this.srcBlobServiceClient = srcBlobServiceClient;
    }

    @Override
    public boolean canHandle(DataFlowRequest dataRequest) {
        return "json".equalsIgnoreCase(dataRequest.getSourceDataAddress().getType().split("_")[0]);
    }

    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        // verify source path
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
        var source = getJson(request);
        return new TransferDataSource(source, request.getSourceDataAddress().getProperty("blobname"));
    }

    @NotNull
    private JSONObject getJson(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        // verify source path
        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);

        String s = srcBlob.downloadContent().toString();

        return new JSONObject(s);
    }

}
