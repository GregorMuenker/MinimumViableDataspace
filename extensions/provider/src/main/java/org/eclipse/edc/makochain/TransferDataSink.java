/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.makochain;

import com.azure.storage.blob.BlobClient;
import io.opentelemetry.extension.annotations.WithSpan;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.util.sink.ParallelSink;
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
                    String malo = output.toString();
                    monitor.info("RequestNewProvider Extension Account " + parts.size());
                    blob.upload(new ByteArrayInputStream(malo.getBytes()), malo.getBytes().length, true);
                    monitor.info("RequestNewProvider Extension upload Blob");
                } catch (Exception e) {
                    return getTransferResult(e, "Error creating blob %s", name);
                }
                monitor.info("RequestNewProvider Extension upload Blob done");
            } catch (Exception e) {
                return getTransferResult(e, "Error reading %s", name);
            }
            monitor.info("RequestNewProvider Extension upload Blob Transfer done");
        }
        // Event success
        monitor.info("RequestNewProvider Extension upload Blob success");
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
        
        public Builder name(String name) {
            sink.name = name;
            return this;
        }

        @Override
        protected void validate() {
            Objects.requireNonNull(sink.blob, "Destination BlobClient missing");
            Objects.requireNonNull(sink.blob, "MaLo missing");
        }

        private Builder() {
            super(new TransferDataSink());
        }
    }
}
