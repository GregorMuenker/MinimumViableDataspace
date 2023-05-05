/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.malonetzbetr;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.json.JSONObject;

import static java.util.Objects.requireNonNull;

public class MaLoProvisionedResource extends ProvisionedResource {
    
    private JSONObject malo;

    public JSONObject getMaLoJsonObject() {
        return malo;
    }

    public static class Builder extends ProvisionedResource.Builder<MaLoProvisionedResource, Builder> {

        protected Builder() {
            super(new MaLoProvisionedResource());
        }
        
        @JsonCreator
        public static Builder newInstance() {
            return new Builder();
        }

        public Builder maLo(JSONObject malo) {
            provisionedResource.malo = malo;
            return this;
        }

        @Override
        protected void verify() {
            requireNonNull(provisionedResource.malo, "MaLo Object missing");
        }
        
        @Override
        public MaLoProvisionedResource build() {
            return super.build();
        }
    }
}