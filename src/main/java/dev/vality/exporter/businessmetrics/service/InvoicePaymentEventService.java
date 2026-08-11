package dev.vality.exporter.businessmetrics.service;

import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.exporter.businessmetrics.handler.invoice.InvoiceEventHandler;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class InvoicePaymentEventService {

    private final List<InvoiceEventHandler> invoiceEventHandlers;
    private final MachineEventParser<EventPayload> parser;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleEvents(List<MachineEvent> machineEvents) {
        machineEvents.forEach(this::handleIfAccept);
    }

    private void handleIfAccept(MachineEvent machineEvent) {
        EventPayload payload = parser.parse(machineEvent);
        payload.getInvoiceChanges().stream()
                .filter(InvoiceChange::isSetInvoicePaymentChange)
                .map(InvoiceChange::getInvoicePaymentChange)
                .forEach(paymentPayload -> invoiceEventHandlers.stream()
                        .filter(handler -> handler.accept(paymentPayload))
                        .forEach(handler -> handler.handle(paymentPayload, machineEvent)));
    }
}
