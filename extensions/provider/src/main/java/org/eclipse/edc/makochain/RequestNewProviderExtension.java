/*
 * Gregor Münker
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

    private static final String PROVIDER_CONTAINER_NAME = "src-container";
    public static final String LOCAL_BLOB_STORE_ENDPOINT_TEMPLATE = "http://127.0.0.1:10000/%s";
    public static final String BLOB_STORE_ACCOUNT = "blob.store.account";
    public static final String BLOB_STORE_ACCOUNT_KEY = "blob.store.accountkey";

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
                format(LOCAL_BLOB_STORE_ENDPOINT_TEMPLATE, context.getSetting(BLOB_STORE_ACCOUNT, "lieferant1assets")),
                    context.getSetting(BLOB_STORE_ACCOUNT, "lieferant1assets"),
                    context.getSetting(BLOB_STORE_ACCOUNT_KEY, "key1")
                );

        var sourceFactory = new TransferDataSourceFactory(blobServiceClient);
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
