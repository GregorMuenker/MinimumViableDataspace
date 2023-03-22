/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.eclipse.edc.connector.contract.spi.negotiation.ConsumerContractNegotiationManager;
//import org.eclipse.edc.connector.contract.spi.negotiation.ConsumerContractNegotiationManager;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataTransferExecutorServiceContainer;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.connector.transfer.spi.TransferProcessManager;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.web.spi.WebService;
import org.jetbrains.annotations.NotNull;

import static java.lang.String.format;

public class RegisterMaLoExtension implements ServiceExtension {

    public static final String LOCAL_BLOB_STORE_ENDPOINT_TEMPLATE = "http://azurite:10000/%s";
    public static final String BLOB_STORE_ACCOUNT = System.getenv("BLOB_STORE_ACCOUNT");
    public static final String BLOB_STORE_ACCOUNT_KEY = System.getenv("BLOB_STORE_ACCOUNT_KEY");

    @Override
    public String name() {
        return "Register MaLo at VNB";
    }
    
    @Inject
    WebService webService;

    @Inject
    private PipelineService pipelineService;
    @Inject
    private DataTransferExecutorServiceContainer executorContainer;

    @Override
    public void initialize(ServiceExtensionContext context) {
        var monitor = context.getMonitor();
        var blobServiceClient = getBlobServiceClient(
                format(LOCAL_BLOB_STORE_ENDPOINT_TEMPLATE, BLOB_STORE_ACCOUNT),
                BLOB_STORE_ACCOUNT,
                BLOB_STORE_ACCOUNT_KEY
                );
        var processManager = context.getService(TransferProcessManager.class);
        var negotiationManager = context.getService(ConsumerContractNegotiationManager.class);

        var sourceFactory = new TransferMaLoSourceFactory(monitor, blobServiceClient, processManager);
        pipelineService.registerFactory(sourceFactory);
        var sinkFactory = new TransferMaLoSinkFactory(monitor, executorContainer.getExecutorService(), 5);
        pipelineService.registerFactory(sinkFactory);

        webService.registerResource(new RegisterMaLoWebservice(context.getMonitor(), negotiationManager, blobServiceClient));

        context.getMonitor().info("Register MaLo Extension initialized!");
    }

    @NotNull
    protected BlobServiceClient getBlobServiceClient(String endpoint, String accountName, String accountKey) {

        return new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();
    }
    //dataspaceconnector:dataflowrequest

}
