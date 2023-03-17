/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.edc.spi.monitor.Monitor;

@Consumes({MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
@Path("/")
public class RequestNewProviderWebservice {

    private final Monitor monitor;

    public RequestNewProviderWebservice(Monitor monitor) {
        this.monitor = monitor;
    }

    @GET
    @Path("wechsel")
    public String checkHealth() {
        monitor.info("Received a change request");
        return "{\"response\":\"change requested\"}";
    }
}
