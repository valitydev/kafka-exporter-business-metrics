package dev.vality.exporter.businessmetrics.handler.invoice;

import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.exporter.businessmetrics.handler.EventHandler;
import dev.vality.machinegun.eventsink.MachineEvent;

public interface InvoiceEventHandler extends EventHandler<InvoicePaymentChange, MachineEvent> {
}
