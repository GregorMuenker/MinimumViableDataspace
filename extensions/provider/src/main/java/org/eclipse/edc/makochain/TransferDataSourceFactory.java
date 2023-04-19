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
    }

    @Override
    public boolean canHandle(DataFlowRequest dataRequest) {
        return "MaLo_lfr".equalsIgnoreCase(dataRequest.getSourceDataAddress().getType());
    }

    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        var dataAddress = request.getSourceDataAddress();
        var blobname = dataAddress.getProperty("blobname");
        var containerName = dataAddress.getProperty("container");
        var datum = request.getProperties().get("date");

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);
        if (!srcBlob.exists()) {
            return Result.failure("Source " + srcBlob.getBlobName() + " does not exist!");
        }

        // TODO validate if can be changed until requested
        String maLo = srcBlob.downloadContent().toString();
        JSONObject jsonMalo = new JSONObject(maLo);

        String beliefertBis = jsonMalo.getString("beliefert_bis");

        LocalDate kuendigungDate = LocalDate.parse(datum);
        LocalDate belieferungsDate = LocalDate.parse(beliefertBis);

        monitor.info("kündigung: " + datum + " | belieferung: " + beliefertBis);
        if (kuendigungDate.isAfter(belieferungsDate)) {
            //TODO Kündigung ist nach Vertragsende
            return Result.success(true);
        } else {
            //TODO Kündigung bei Belieferung
            // return Result.failure("wird noch beliefert")

            return Result.success(true);
        }
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var malo = getMaLoInfo(request);
        return new TransferDataSource(malo, request.getSourceDataAddress().getProperty("blobname"));
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
