package dev.vality.exporter.businessmetrics.converter;

import dev.vality.damsel.payment_processing.InvoicePaymentChangePayload;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.machinegun.eventsink.MachineEvent;


public interface InvoiceEventConverter {

    PaymentEvent convert(MachineEvent machineEvent, InvoicePaymentChangePayload payload);

    boolean isConvert(InvoicePaymentChangePayload payload);

}
