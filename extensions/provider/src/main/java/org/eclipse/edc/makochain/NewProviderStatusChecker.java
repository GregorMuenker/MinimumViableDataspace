package org.eclipse.edc.makochain;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.StatusChecker;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;

import java.time.Clock;
import java.util.List;

public class NewProviderStatusChecker implements StatusChecker {

    private final BlobServiceClient blobServiceClient;

    public NewProviderStatusChecker(BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }

    @Override
    public boolean isComplete(TransferProcess transferProcess, List<ProvisionedResource> resources) {
        var dataDest = transferProcess.getDataRequest().getDataDestination();
        var container = blobServiceClient.getBlobContainerClient(dataDest.getProperty("container"));
        for (BlobItem blob : container.listBlobs()) {
            if (blob.getName() == dataDest.getProperty("blobname")) {
                return true;
            }
        }
        if (transferProcess.getCreatedAt() + 300000 < Clock.systemUTC().millis()) { //check if 5 min have passed
            transferProcess.transitionTerminated("no response from vnb");
        }
        return false;
    }
    
}
