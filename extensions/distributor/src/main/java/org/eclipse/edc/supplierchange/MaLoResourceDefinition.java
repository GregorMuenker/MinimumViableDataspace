/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;

import java.util.Objects;

public class MaLoResourceDefinition extends ResourceDefinition {

    private String maloId;

    public String getmaloId() {
        return maloId;
    }

    @Override
    public Builder toBuilder() {
        return initializeBuilder(new Builder().maloId(maloId));
    }

    public static class Builder extends ResourceDefinition.Builder<MaLoResourceDefinition, Builder> {

        private Builder() {
            super(new MaLoResourceDefinition());
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder maloId(String id) {
            resourceDefinition.maloId = id;
            return this;
        }

        @Override
        protected void verify() {
            super.verify();
            Objects.requireNonNull(resourceDefinition.maloId, "maLo");
        }

    }

}