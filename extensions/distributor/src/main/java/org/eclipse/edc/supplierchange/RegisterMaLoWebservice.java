/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.edc.catalog.spi.Catalog;
import org.eclipse.edc.connector.contract.spi.negotiation.ConsumerContractNegotiationManager;
import org.eclipse.edc.connector.contract.spi.types.negotiation.ContractOfferRequest;
import org.eclipse.edc.connector.contract.spi.types.offer.ContractOffer;
import org.eclipse.edc.connector.spi.catalog.CatalogService;
import org.eclipse.edc.connector.spi.contractnegotiation.ContractNegotiationService;
import org.eclipse.edc.policy.model.Action;
import org.eclipse.edc.policy.model.Permission;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.policy.model.PolicyType;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.query.Criterion;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.types.domain.asset.Asset;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Consumes({ MediaType.APPLICATION_JSON })
@Produces({ MediaType.APPLICATION_JSON })
@Path("/")
public class RegisterMaLoWebservice {

    private final Monitor monitor;
    private final ConsumerContractNegotiationManager consumerNegotiationManager;
    private BlobServiceClient srcBlobServiceClient;
    private final CatalogService catalogService;
    private final ContractNegotiationService negotiationService;
    
    public RegisterMaLoWebservice(Monitor monitor, ConsumerContractNegotiationManager consumerNegotiationManager, BlobServiceClient srcBlobServiceClient, CatalogService catalogService, ContractNegotiationService negotiationService) {
        this.monitor = monitor;
        this.consumerNegotiationManager = consumerNegotiationManager;
        this.srcBlobServiceClient = srcBlobServiceClient;
        this.catalogService = catalogService;
        this.negotiationService = negotiationService;
    }

    @GET
    @Path("wechsel")
    public String checkHealth() {
        monitor.info("Received a change request");
        return "{\"response\":\"change requested\"}";
    }

    @POST
    @Path("negotiateAll")
    public void initiateNegotiation(@Suspended AsyncResponse response) {
        negotiateAll(response);
    }

    private void negotiateAll(AsyncResponse response) {
        var maLoBlobContainer = srcBlobServiceClient.getBlobContainerClient("src-container");
        List<CompletableFuture<Catalog>> futures = new ArrayList<>();
        for (BlobItem blobItem : maLoBlobContainer.listBlobs()) {
            BlobClient blobClient = maLoBlobContainer.getBlobClient(blobItem.getName());
            JSONObject malo = new JSONObject(blobClient.downloadContent().toString());
            JSONObject lieferantMalo = malo.getJSONObject("lieferant");
            List<Criterion> criteria = new ArrayList<>();
            criteria.add(new Criterion("type", "=", "MaLo_lfr"));
            criteria.add(new Criterion(Asset.PROPERTY_NAME, "=", "MaLo_" + malo.getString("maLo") + "_" + lieferantMalo.getString("name")));
            QuerySpec query = QuerySpec.Builder.newInstance().filter(criteria).build();
            futures.add(catalogService.getByProviderUrl(lieferantMalo.getString("connector"), query)
                    .whenComplete((content, throwable) -> {
                        if (throwable == null) {
                            List<ContractOffer> offers = content.getContractOffers();
                            if (offers.size() == 1) {
                                var offer = offers.get(0); 
                                var contractOffer = ContractOffer.Builder.newInstance()
                                        .id(offer.getId())
                                        .policy(Policy.Builder.newInstance()
                                            .type(PolicyType.SET)
                                            .target(offer.getPolicy().getTarget())
                                            .permission(Permission.Builder.newInstance()
                                                .target(offer.getPolicy().getTarget())
                                                .action(Action.Builder.newInstance()
                                                    .type("USE")
                                                    .build())
                                                .build())
                                            .build())
                                        .asset(offer.getAsset())
                                        .contractStart(ZonedDateTime.now())
                                        .contractEnd(ZonedDateTime.now().plusDays(365))
                                        .consumer(URI.create("vnb"))
                                        .provider(URI.create(lieferantMalo.getString("name")))
                                        .build();
    
                                var contractOfferRequest = ContractOfferRequest.Builder.newInstance()
                                        .contractOffer(contractOffer)
                                        .protocol("ids-multipart")
                                        .connectorId(lieferantMalo.getString("name"))
                                        .connectorAddress(lieferantMalo.getString("connector"))
                                        .type(ContractOfferRequest.Type.INITIAL)
                                        .build();
    
                                var result = consumerNegotiationManager.initiate(contractOfferRequest);
                                if (result.failed()) {
                                    response.resume("error");
                                }

                                try {
                                    //monitor.info("sleep start");
                                    Thread.sleep(5000); // wait for negotiation
                                    //monitor.info("sleep end");
                                } catch (Exception e) {
                                    // TODO: handle exception
                                }
                                
                                var nagotiation = negotiationService.findbyId(result.getContent().getId());
                                lieferantMalo.put("dataContract", nagotiation.getContractAgreement().getId());
                                malo.put("lieferant", lieferantMalo);
                                byte[] bytes = malo.toString().getBytes();
                                ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
                                blobClient.upload(inputStream, bytes.length, true);
                            }
                        } else {
                            response.resume(throwable);
                        }
                        response.resume("failed to negotiate");
                    })
            );
        }
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );
        allFutures.join();
        allFutures.whenComplete((result, error) -> {
            response.resume("done");
        });
    }

}

