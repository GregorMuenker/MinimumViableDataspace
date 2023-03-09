/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.makochain;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataTransferExecutorServiceContainer;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.web.spi.WebService;
import org.jetbrains.annotations.NotNull;

import static java.lang.String.format;

public class RequestNewProviderExtension implements ServiceExtension {

    public static final String LOCAL_BLOB_STORE_ENDPOINT_TEMPLATE = "http://azurite:10000/%s";
    public static final String BLOB_STORE_ACCOUNT = System.getenv("BLOB_STORE_ACCOUNT");
    public static final String BLOB_STORE_ACCOUNT_KEY = System.getenv("BLOB_STORE_ACCOUNT_KEY");

    @Override
    public String name() {
        return "Request new Provider";
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
        context.getMonitor().info("RequestNewProvider Extension " + format(LOCAL_BLOB_STORE_ENDPOINT_TEMPLATE, BLOB_STORE_ACCOUNT));

        var sourceFactory = new TransferDataSourceFactory(monitor, blobServiceClient);
        pipelineService.registerFactory(sourceFactory);

        var sinkFactory = new TransferDataSinkFactory(monitor, executorContainer.getExecutorService(), 5, blobServiceClient);
        pipelineService.registerFactory(sinkFactory);

        webService.registerResource(new RequestNewProvider(context.getMonitor()));

        context.getMonitor().info("RequestNewProvider Extension initialized!");
    }

    @NotNull
    protected BlobServiceClient getBlobServiceClient(String endpoint, String accountName, String accountKey) {

        return new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();
    }

}
