package dev.vality.exporter.businessmetrics.converter;

import dev.vality.damsel.payment_processing.InvoicePaymentChangePayload;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class InvoicePaymentStartedToPaymentEventConverter implements InvoiceEventConverter {

    @Override
    public PaymentEvent convert(MachineEvent machineEvent, InvoicePaymentChangePayload payload) {
        var invoicePaymentStarted = payload.getInvoicePaymentStarted();
        var payment = invoicePaymentStarted.getPayment();
        PaymentEvent event = new PaymentEvent();
        event.setInvoiceId(machineEvent.getSourceId());
        event.setProviderId(invoicePaymentStarted.getRoute().getProvider().getId());
        event.setTerminalId(invoicePaymentStarted.getRoute().getTerminal().getId());
        event.setShopId(payment.getShopRef().getId());
        event.setCurrencyCode(payment.getCost().getCurrency().getSymbolicCode());
        event.setAmount(payment.getCost().getAmount());
        event.setStatus(payment.getStatus().getSetField().getFieldName());
        event.setCreatedAt(Instant.parse(machineEvent.getCreatedAt()));
        return event;
    }

    @Override
    public boolean isApplicable(InvoicePaymentChangePayload payload) {
        return payload.isSetInvoicePaymentStarted();
    }
}
