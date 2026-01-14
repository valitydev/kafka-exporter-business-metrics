package dev.vality.exporter.businessmetrics.converter;

import dev.vality.damsel.payment_processing.InvoicePaymentChangePayload;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class InvoicePaymentStatusChangedToPaymentEventConverter implements InvoiceEventConverter {

    @Override
    public PaymentEvent convert(MachineEvent machineEvent, InvoicePaymentChangePayload payload) {
        var invoicePaymentStatusChanged = payload.getInvoicePaymentStatusChanged();
        PaymentEvent event = new PaymentEvent();
        event.setInvoiceId(machineEvent.getSourceId());
        event.setStatus(invoicePaymentStatusChanged.getStatus().getSetField().getFieldName());
        event.setCreatedAt(Instant.parse(machineEvent.getCreatedAt()));
        return event;
    }

    @Override
    public boolean isConvert(InvoicePaymentChangePayload payload) {
        return payload.isSetInvoicePaymentStatusChanged();
    }
}
