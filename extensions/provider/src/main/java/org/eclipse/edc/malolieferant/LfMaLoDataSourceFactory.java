/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.malolieferant;

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

public class LfMaLoDataSourceFactory implements DataSourceFactory {

    private BlobServiceClient srcBlobServiceClient;
    private final Monitor monitor;

    LfMaLoDataSourceFactory(Monitor monitor, BlobServiceClient srcBlobServiceClient) {
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
        JSONObject contract = maLo.getJSONObject("contract");
        LocalDate contractEndDate = LocalDate.parse(contract.getString("contractEnd"));
        String contractPeriod = contract.getString("cyclePeriod");
        switch (contractPeriod) {
            case "m": 
                contractCancelOk = LocalDate.of(LocalDate.now().getYear(), LocalDate.now().getMonth(), LocalDate.now().lengthOfMonth()).isBefore(kuendigungDate); 
                break;
            case "y": 
                contractCancelOk = LocalDate.of(LocalDate.now().getYear(), 12, 31).isBefore(kuendigungDate); 
                break;
            default: 
                contractCancelOk = false; 
                break;
        }

        monitor.info("kündigung: " + datum + " | belieferung: " + contractEndDate);
        if (contractCancelOk) {
            //Kündigung ist möglich 

            //TODO Kundenmitteilung

            contract.put("contractEnd", kuendigungDate);
            maLo.put("contract", contract);
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
        JSONObject contract = new JSONObject(malo).getJSONObject("contract");
        JSONObject transferredInfo = new JSONObject();
        transferredInfo.put("end_date", contract.getString("contractEnd"));

        return new LfMaLoDataSource(transferredInfo.toString(), request.getSourceDataAddress().getProperty("blobname"));
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
