/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.makochain;

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.stream.Stream;

class TransferDataSource implements DataSource {

    private final String maLo;
    private final String name;

    TransferDataSource(String maLo, String name) {
        this.maLo = maLo;
        this.name = name;
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
                    return new ByteArrayInputStream(maLo.getBytes());
                } catch (Exception e) {
                    throw new EdcException(e);
                }
            }
        });
    }
}
