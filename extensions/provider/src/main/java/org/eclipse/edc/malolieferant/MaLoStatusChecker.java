package org.eclipse.edc.malolieferant;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.StatusChecker;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
import org.eclipse.edc.spi.monitor.Monitor;

import java.time.Clock;
import java.util.List;

public class MaLoStatusChecker implements StatusChecker {

    private final BlobServiceClient blobServiceClient;
    private final Monitor monitor;

    public MaLoStatusChecker(Monitor monitor, BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
        this.monitor = monitor;
    }

    @Override
    public boolean isComplete(TransferProcess transferProcess, List<ProvisionedResource> resources) {
        var dataDest = transferProcess.getDataRequest().getDataDestination();
        var container = blobServiceClient.getBlobContainerClient(dataDest.getProperty("container"));
        for (BlobItem blob : container.listBlobs()) {
            monitor.info(blob.getName());
            if (blob.getName().equalsIgnoreCase(dataDest.getProperty("blobname"))) {
                return true;
            }
        }
        if (transferProcess.getCreatedAt() + 300000 < Clock.systemUTC().millis()) { //check if 5 min have passed
            transferProcess.transitionTerminated("no response from vnb");
        }
        return false;
    }
    
}
