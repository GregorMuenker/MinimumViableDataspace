package org.eclipse.edc.supplierchange;

import org.eclipse.edc.spi.event.Event;
import org.eclipse.edc.spi.event.EventEnvelope;
import org.eclipse.edc.spi.event.EventSubscriber;

public class TransferEventSubscriber implements EventSubscriber<Event>{

    public void on(EventEnvelope<Event> event) {
        // react to event    
    }
    
}
