/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

package org.eclipse.edc.supplierchange;

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.spi.EdcException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.stream.Stream;

class TransferMaLoSource implements DataSource {

    private final String maLo;
    private final String name;

    TransferMaLoSource(String maLo, String name) {
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
