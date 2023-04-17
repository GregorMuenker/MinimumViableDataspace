/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;

import org.json.JSONObject;

import static java.util.Objects.requireNonNull;

public class MaLoProvisionedResource extends ProvisionedResource {
    
    protected String resourceName;
    protected JSONObject malo;

    public String getResourceName() {
        return resourceName;
    }

    public static class Builder<T extends MaLoProvisionedResource, B extends Builder<T, B>> extends ProvisionedResource.Builder<T, B> {

        @SuppressWarnings("unchecked")
        public B resourceName(String name) {
            provisionedResource.resourceName = name;
            return (B) this;
        }

        @SuppressWarnings("unchecked")
        public B maLo(JSONObject malo) {
            provisionedResource.malo = malo;
            return (B) this;
        }

        protected Builder(T resource) {
            super(resource);
        }

        @Override
        public T build() {
            return super.build();
        }

        @Override
        protected void verify() {
            requireNonNull(provisionedResource.resourceName, "resourceName");
            requireNonNull(provisionedResource.malo, "maLo");
        }
    }

}
