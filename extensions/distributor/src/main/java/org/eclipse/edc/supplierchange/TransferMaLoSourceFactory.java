/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

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
        var maloBlob = getMaLoBlob(request);
        var malo = new JSONObject(maloBlob.downloadContent().toString());

        //save the new supplier
        var lieferungen = malo.getJSONArray("belieferungen");
        var lieferungNeu = new JSONObject();
        var lieferantNeu = new JSONObject();
        lieferungNeu.put("von", request.getProperties().get("start_date"));
        lieferungNeu.put("bis", request.getProperties().get("end_date"));
        lieferantNeu.put("name", request.getProperties().get("supplier"));
        lieferantNeu.put("connector", request.getCallbackAddress()); //TODO: get Lieferant ID
        lieferantNeu.put("dataContract", "");
        lieferungNeu.put("lieferant", lieferantNeu);    
        lieferungen.put(lieferungNeu);
        malo.put("belieferungen", lieferungen);

        maloBlob.upload(BinaryData.fromString(malo.toString()), true);
        BlobHttpHeaders headers = new BlobHttpHeaders().setContentType("application/json");
        maloBlob.setHttpHeaders(headers);

        //only transfer relevant infos
        var maloSupplier = new JSONObject();
        maloSupplier.put("maLo", malo.getString("maLo"));
        maloSupplier.put("name", malo.getJSONObject("name"));
        maloSupplier.put("address", malo.getJSONObject("address"));

        return new TransferMaLoSource(maloSupplier.toString(), request.getSourceDataAddress().getProperty("blobname"));
    }

    @NotNull
    private BlobClient getMaLoBlob(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");

        return srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);
    }

}
