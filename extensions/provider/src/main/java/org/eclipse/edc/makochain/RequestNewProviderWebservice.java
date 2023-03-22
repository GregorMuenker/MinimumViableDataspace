/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.makochain;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
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
import org.eclipse.edc.connector.transfer.spi.TransferProcessManager;
import org.eclipse.edc.connector.transfer.spi.types.DataRequest;
import org.eclipse.edc.policy.model.Action;
import org.eclipse.edc.policy.model.Duty;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.policy.model.PolicyType;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.asset.Asset;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;

import static org.eclipse.edc.spi.response.ResponseStatus.FATAL_ERROR;

@Consumes({ MediaType.APPLICATION_JSON })
@Produces({ MediaType.APPLICATION_JSON })
@Path("/")
public class RequestNewProviderWebservice {

    private final Monitor monitor;
    private final TransferProcessManager processManager;
    private final ConsumerContractNegotiationManager consumerNegotiationManager;
    private BlobServiceClient srcBlobServiceClient;
    private String lastContractId = "";

    public RequestNewProviderWebservice(Monitor monitor, TransferProcessManager processManager,
            ConsumerContractNegotiationManager consumerNegotiationManager, BlobServiceClient srcBlobServiceClient) {
        this.monitor = monitor;
        this.processManager = processManager;
        this.consumerNegotiationManager = consumerNegotiationManager;
        this.srcBlobServiceClient = srcBlobServiceClient;
    }

    @POST
    @Path("wechsel")
    public String wechselLieferant() {
        monitor.info("Received a change request");

        return "{\"response\":\"change requested\"}";
    }

    @GET
    @Path("malos")
    public String getMaLos() {
        monitor.info("Request MaLos");

        return "{\"response\":\"MaLos\"}";
    }

    @POST
    @Path("negotiation")
    public Response initiateNegotiation() {

        String maLo = "MaLo_12345678902";

        var contractOffer = ContractOffer.Builder.newInstance()
                .id(ContractId.createContractId("1"))
                .policy(Policy.Builder.newInstance()
                        .type(PolicyType.SET)
                        .target(maLo)
                        .duty(Duty.Builder.newInstance()
                                .action(Action.Builder.newInstance()
                                        .type("USE")
                                        .build())
                                .build())
                        .build())
                .asset(Asset.Builder.newInstance()
                        .property("asset:prop:name", maLo)
                        .property("asset:prop:id", maLo)
                        .property("type", "MaLo")
                        .build())
                .contractStart(ZonedDateTime.now())
                .contractEnd(ZonedDateTime.now().plusHours(3))
                .build();

        var contractOfferRequest = ContractOfferRequest.Builder.newInstance()
                .contractOffer(contractOffer)
                .protocol("ids-multipart")
                .connectorId("vnb")
                .connectorAddress("http://vnb:8282/api/v1/ids/data")
                .build();

        var result = consumerNegotiationManager.initiate(contractOfferRequest);
        if (result.failed() && result.getFailure().status() == FATAL_ERROR) {
            return Response.serverError().build();
        }
        lastContractId = result.getContent().getId();
        return Response.ok(lastContractId).build();
    }

    @POST
    @Path("transfer")
    public Response initiateTransfer() {

        OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
        BlobContainerSasPermission permission = new BlobContainerSasPermission()
                .setWritePermission(true)
                .setAddPermission(true)
                .setCreatePermission(true);

        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiryTime, permission)
                .setStartTime(OffsetDateTime.now());

        var dataRequest = DataRequest.Builder.newInstance()
                .id(UUID.randomUUID().toString()) // this is not relevant, thus can be random
                .connectorAddress("http://vnb:8282/api/v1/ids/data") // the address of the provider connector
                .protocol("ids-multipart")
                .connectorId("vnb")
                .assetId("MaLo_12345678902")
                .dataDestination(DataAddress.Builder.newInstance()
                        .type("MaLo") // the provider uses this to select the correct DataFlowController
                        .property("container", "src-container")
                        .property("blobname", "lieferant1_customer2_lieferant2")
                        .property("account", "lieferant1assets")
                        .property("sasToken",
                                srcBlobServiceClient.getBlobContainerClient("src-container").generateSas(values))
                        .build())
                .managedResources(false) // we do not need any provisioning
                .contractId(lastContractId)
                .build();

        var result = processManager.initiateConsumerRequest(dataRequest);

        return result.failed() ? Response.status(400).build() : Response.ok(result.getContent()).build();
    }
}
