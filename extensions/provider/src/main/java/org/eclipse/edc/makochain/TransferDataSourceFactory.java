/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.makochain;

import com.azure.core.util.BinaryData;
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
        LocalDate kuendigungDate = LocalDate.parse(datum);

        BlobClient srcBlob = srcBlobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobname);
        if (!srcBlob.exists()) {
            return Result.failure("Source " + srcBlob.getBlobName() + " does not exist!");
        }
        JSONObject maLo = new JSONObject(srcBlob.downloadContent().toString());

        //check if contract can be cancelled until kündigungsDate
        boolean contractCancelOk = false;
        JSONObject vertrag = maLo.getJSONObject("vertrag");
        LocalDate contractEndDate = LocalDate.parse(vertrag.getString("contractEnd"));
        String contractPeriod = vertrag.getString("cyclePeriod");
        switch (contractPeriod) {
            case "m": 
                contractCancelOk = LocalDate.of(contractEndDate.getYear(), contractEndDate.getMonth(), contractEndDate.lengthOfMonth()).isBefore(kuendigungDate); 
                break;
            case "y": 
                contractCancelOk = LocalDate.of(contractEndDate.getYear(), 12, 31).isBefore(kuendigungDate); 
                break;
            default: 
                contractCancelOk = false; 
                break;
        }

        monitor.info("kündigung: " + datum + " | belieferung: " + contractEndDate);
        if (contractCancelOk) {
            //Kündigung ist möglich 

            //TODO Kundenmitteilung

            vertrag.put("contractEnd", kuendigungDate);
            maLo.put("vertrag", vertrag);
            srcBlob.upload(BinaryData.fromString(maLo.toString()), true);

            return Result.success(true);
        } else {
            //Kündigung ist nicht möglich
            return Result.failure("Kündigung nicht möglich");
        }
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var malo = getMaLoInfo(request);
        JSONObject vertrag = new JSONObject(malo).getJSONObject("vertrag");
        JSONObject transferredInfo = new JSONObject();
        transferredInfo.put("end_date", vertrag.getString("contractEnd"));

        return new TransferDataSource(transferredInfo.toString(), request.getSourceDataAddress().getProperty("blobname"));
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
