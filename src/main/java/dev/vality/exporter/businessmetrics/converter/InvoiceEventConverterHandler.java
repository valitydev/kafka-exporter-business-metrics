package dev.vality.exporter.businessmetrics.converter;

import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class InvoiceEventConverterHandler {

    private final List<InvoiceEventConverter> converters;

    public List<PaymentEvent> handle(MachineEvent event, EventPayload payload) {
        return payload.getInvoiceChanges().stream()
                .filter(InvoiceChange::isSetInvoicePaymentChange)
                .map(change -> change.getInvoicePaymentChange().getPayload())
                .flatMap(paymentPayload ->
                        converters.stream()
                                .filter(c -> c.isApplicable(paymentPayload))
                                .map(c -> c.convert(event, paymentPayload))
                )
                .toList();
    }
}
