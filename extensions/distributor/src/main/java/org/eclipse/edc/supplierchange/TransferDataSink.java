/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import com.azure.storage.blob.BlobClient;
import io.opentelemetry.extension.annotations.WithSpan;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.util.sink.ParallelSink;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static org.eclipse.edc.spi.response.ResponseStatus.ERROR_RETRY;

public class TransferDataSink extends ParallelSink {
    private BlobClient blob;
    private String name;
    private Monitor monitorr;

    @WithSpan
    @Override
    protected StatusResult<Void> transferParts(List<DataSource.Part> parts) {
        for (DataSource.Part part : parts) {
            try (var input = part.openStream()) {
                try (var output = new ByteArrayOutputStream()) {
                    try {
                        input.transferTo(output);
                    } catch (Exception e) {
                        return getTransferResult(e, "Error transferring %s", name);
                    }
                    String json = output.toString();
                    monitorr.info("RequestNewProvider Extension upload Blob");
                    monitorr.info("RequestNewProvider Extension Blob " + json);
                    monitorr.info("RequestNewProvider Extension Account " + blob.getAccountName());
                    blob.upload(new ByteArrayInputStream(json.getBytes()), json.getBytes().length, true);
                    monitorr.info("RequestNewProvider Extension Url " + blob.getBlobUrl());
                } catch (Exception e) {
                    return getTransferResult(e, "Error creating blob %s", name);
                }
            } catch (Exception e) {
                return getTransferResult(e, "Error reading %s", name);
            }
        }
        return StatusResult.success();
    }

    private StatusResult<Void> getTransferResult(Exception e, String logMessage, Object... args) {
        var message = format(logMessage, args);
        monitor.severe(message, e);
        return StatusResult.failure(ERROR_RETRY, message);
    }

    public static class Builder extends ParallelSink.Builder<Builder, TransferDataSink> {

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder blob(BlobClient blob) {
            sink.blob = blob;
            return this;
        }

        public Builder monitor(Monitor monitorr) {
            sink.monitorr = monitorr;
            return this;
        }
        
        public Builder name(String name) {
            sink.name = name;
            return this;
        }

        @Override
        protected void validate() {
            Objects.requireNonNull(sink.blob, "json");
        }

        private Builder() {
            super(new TransferDataSink());
        }
    }
}
