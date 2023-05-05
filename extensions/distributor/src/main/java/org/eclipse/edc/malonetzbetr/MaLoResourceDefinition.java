/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.malonetzbetr;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;

import java.util.Objects;

public class MaLoResourceDefinition extends ResourceDefinition {

    private BlobClient maLo;
    private String requestedStartDate;
    private String requestedEndDate;
    private BlobContainerClient tempContainer;

    public BlobClient getMaloBlob() {
        return maLo;
    }

    public String getRequestedStartDate() {
        return requestedStartDate;
    }

    public String getRequestedEndDate() {
        return requestedEndDate;
    }

    public BlobContainerClient getTempContainer() {
        return tempContainer;
    }

    @Override
    public Builder toBuilder() {
        return initializeBuilder(new Builder().maloBlob(maLo));
    }

    public static class Builder extends ResourceDefinition.Builder<MaLoResourceDefinition, Builder> {

        private Builder() {
            super(new MaLoResourceDefinition());
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder maloBlob(BlobClient maLo) {
            resourceDefinition.maLo = maLo;
            return this;
        }

        public Builder requestedStartDate(String requestedDate) {
            resourceDefinition.requestedStartDate = requestedDate;
            return this;
        }

        public Builder requestedEndDate(String requestedDate) {
            resourceDefinition.requestedEndDate = requestedDate;
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
            Objects.requireNonNull(resourceDefinition.requestedStartDate, "Requested Start Date missing");
            Objects.requireNonNull(resourceDefinition.requestedEndDate, "Requested End Date missing");
            Objects.requireNonNull(resourceDefinition.tempContainer, "Temp ContainerClient missing");
        }
    }
}