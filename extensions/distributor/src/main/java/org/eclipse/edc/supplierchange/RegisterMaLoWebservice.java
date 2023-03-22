/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.edc.connector.contract.spi.ContractId;
import org.eclipse.edc.connector.contract.spi.negotiation.ConsumerContractNegotiationManager;
import org.eclipse.edc.connector.contract.spi.types.negotiation.ContractOfferRequest;
import org.eclipse.edc.connector.contract.spi.types.offer.ContractOffer;
import org.eclipse.edc.policy.model.Action;
import org.eclipse.edc.policy.model.Duty;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.policy.model.PolicyType;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.domain.asset.Asset;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.time.ZonedDateTime;

import static org.eclipse.edc.spi.response.ResponseStatus.FATAL_ERROR;

@Consumes({MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
@Path("/")
public class RegisterMaLoWebservice {

    private final Monitor monitor;
    private final ConsumerContractNegotiationManager consumerNegotiationManager;
    private BlobServiceClient srcBlobServiceClient;

    public RegisterMaLoWebservice(Monitor monitor, ConsumerContractNegotiationManager consumerNegotiationManager, BlobServiceClient srcBlobServiceClient) {
        this.monitor = monitor;
        this.consumerNegotiationManager = consumerNegotiationManager;
        this.srcBlobServiceClient = srcBlobServiceClient;
    }

    @GET
    @Path("wechsel")
    public String checkHealth() {
        monitor.info("Received a change request");
        return "{\"response\":\"change requested\"}";
    }

    @POST
    @Path("negotiation")
    public Response initiateNegotiation() {

        String maLo = "MaLo_12345678902";

        var contractOffer = ContractOffer.Builder.newInstance()
                .id(ContractId.createContractId("1"))
                .policy(Policy.Builder.newInstance()
                        .type(PolicyType.SET)
                        .target(maLo + "_lieferant2")
                        .duty(Duty.Builder.newInstance()
                                .action(Action.Builder.newInstance()
                                        .type("USE")
                                        .build())
                                .build())
                        .build())
                .asset(Asset.Builder.newInstance()
                        .property("asset:prop:name", maLo + "_lieferant2")
                        .property("asset:prop:id", maLo + "_lieferant2")
                        .property("type", "MaLo_end")
                        .build())
                .contractStart(ZonedDateTime.now())
                .contractEnd(ZonedDateTime.now().plusHours(3))
                .build();

        var contractOfferRequest = ContractOfferRequest.Builder.newInstance()
                .contractOffer(contractOffer)
                .protocol("ids-multipart")
                .connectorId("lieferant2")
                .connectorAddress("http://lieferant2:8282/api/v1/ids/data")
                .build();

        var result = consumerNegotiationManager.initiate(contractOfferRequest);
        if (result.failed() && result.getFailure().status() == FATAL_ERROR) {
            return Response.serverError().build();
        }

        BlobClient blobMaLo = srcBlobServiceClient.getBlobContainerClient("src-container").getBlobClient("vnb_" + maLo + ".json");
        JSONObject jsonMalo = new JSONObject(blobMaLo.downloadContent().toString());

        String updatedMalo = jsonMalo.put("lieferant", jsonMalo.getJSONObject("lieferant").put("dataContract", result.getContent().getId())).toString();

        blobMaLo.upload(new ByteArrayInputStream(updatedMalo.getBytes()), updatedMalo.getBytes().length, true);

        return Response.ok(result.getContent().getId()).build();
    }
}
