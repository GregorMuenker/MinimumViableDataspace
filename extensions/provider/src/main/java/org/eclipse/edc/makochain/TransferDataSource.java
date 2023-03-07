/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.makochain;

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.stream.Stream;

class TransferDataSource implements DataSource {

    private final JSONObject json;
    private final String name;
    private final Monitor monitor;

    TransferDataSource(Monitor monitor, JSONObject json, String name) {
        this.json = json;
        this.name = name;
        this.monitor = monitor;
    }

    @Override
    public Stream<Part> openPartStream() {
        return Stream.of(new Part() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public InputStream openStream() {
                try {
                    monitor.info("Source open Stream");
                    return new ByteArrayInputStream(json.toString().getBytes());
                } catch (Exception e) {
                    throw new EdcException(e);
                }
            }
        });
    }
}
