/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobContainerClient;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.json.JSONObject;

import java.util.Objects;

public class MaLoResourceDefinition extends ResourceDefinition {

    private JSONObject maLo;
    private String requestDate;
    private BlobContainerClient tempContainer;

    public JSONObject getmaLo() {
        return maLo;
    }

    public String getRequestDate() {
        return requestDate;
    }

    public BlobContainerClient getTempContainer() {
        return tempContainer;
    }

    @Override
    public Builder toBuilder() {
        return initializeBuilder(new Builder().maLo(maLo));
    }

    public static class Builder extends ResourceDefinition.Builder<MaLoResourceDefinition, Builder> {

        private Builder() {
            super(new MaLoResourceDefinition());
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder maLo(JSONObject maLo) {
            resourceDefinition.maLo = maLo;
            return this;
        }

        public Builder requestDate(String requestDate) {
            resourceDefinition.requestDate = requestDate;
            return this;
        }
        
        public Builder tempContainer(BlobContainerClient tempContainer) {
            resourceDefinition.tempContainer = tempContainer;
            return this;
        }

        @Override
        protected void verify() {
            super.verify();
            Objects.requireNonNull(resourceDefinition.maLo, "MaLo Object missing");
            Objects.requireNonNull(resourceDefinition.requestDate, "Requested Change Date missing");
            Objects.requireNonNull(resourceDefinition.tempContainer, "Temp ContainerClient missing");
        }

    }

}