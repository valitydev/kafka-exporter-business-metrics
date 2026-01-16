package dev.vality.exporter.businessmetrics.converter;

import dev.vality.damsel.payment_processing.InvoicePaymentChangePayload;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class InvoicePaymentRouteChangedToPaymentEventConverter implements InvoiceEventConverter {

    @Override
    public PaymentEvent convert(MachineEvent machineEvent, InvoicePaymentChangePayload payload) {
        var invoicePaymentRouteChanged = payload.getInvoicePaymentRouteChanged();
        var route = invoicePaymentRouteChanged.getRoute();
        PaymentEvent event = new PaymentEvent();
        event.setInvoiceId(machineEvent.getSourceId());
        event.setProviderId(route.getProvider().getId());
        event.setTerminalId(route.getTerminal().getId());
        event.setCreatedAt(Instant.parse(machineEvent.getCreatedAt()));
        return event;
    }

    @Override
    public boolean isApplicable(InvoicePaymentChangePayload payload) {
        return payload.isSetInvoicePaymentRouteChanged();
    }
}
