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

import java.time.LocalDate;

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
        monitor.info("RequestNewProvider Extension Source Factory canhandle " + dataRequest.getSourceDataAddress().getType());
        return "MaLo_flr".equalsIgnoreCase(dataRequest.getSourceDataAddress().getType());
    }

    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        // verify source path
        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");

        var destDataAddress = request.getDestinationDataAddress();
        var datum = destDataAddress.getProperty("date");

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);

        if (!srcBlob.exists()) {
            return Result.failure("Source " + srcBlob.getBlobName() + " does not exist!");
        }

        // ToDo validate if can be changed until requested
        String maLo = srcBlob.downloadContent().toString();
        JSONObject jsonMalo = new JSONObject(maLo);

        String beliefertBis = jsonMalo.optString("beliefert_bis");

        LocalDate kuendigungDate = LocalDate.parse(datum);
        LocalDate belieferungsDate = LocalDate.parse(beliefertBis);

        if (kuendigungDate.isAfter(belieferungsDate)) {
            // Kündigung ist nach Vertragsende
        } else {
            // Kündigung bei Belieferung
            return Result.success(false);
        }

        monitor.info("RequestNewProvider Extension Source validate true " + srcBlob.getBlobName());
        return Result.success(true);
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var malo = getMaLoInfo(request);
        return new TransferDataSource(monitor, malo, request.getSourceDataAddress().getProperty("blobname"));
    }

    @NotNull
    private String getMaLoInfo(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        // verify source path
        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");

        monitor.info("RequestNewProvider Extension Source Json " + blobname + " : " + containerName);

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);

        monitor.info("RequestNewProvider Extension Source Json" + srcBlob.getBlobName() + " !");

        return srcBlob.downloadContent().toString();
    }

}
