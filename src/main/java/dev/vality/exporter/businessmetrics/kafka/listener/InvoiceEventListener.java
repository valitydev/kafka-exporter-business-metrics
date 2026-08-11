package dev.vality.exporter.businessmetrics.kafka.listener;

import dev.vality.exporter.businessmetrics.service.InvoicePaymentEventService;
import dev.vality.machinegun.eventsink.SinkEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Slf4j
@Service
@RequiredArgsConstructor
public class InvoiceEventListener {

    private final InvoicePaymentEventService invoicePaymentEventService;

    @KafkaListener(
            autoStartup = "${kafka.topics.invoice.enabled}",
            topics = "${kafka.topics.invoice.id}",
            containerFactory = "invoicingListenerContainerFactory")
    public void listen(
            List<SinkEvent> batch,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) int offset,
            Acknowledgment ack) {
        log.info("Listening Invoice: partition={}, offset={}, batch.size()={}", partition, offset, batch.size());
        invoicePaymentEventService.handleEvents(batch.stream().map(SinkEvent::getEvent).collect(toList()));
        ack.acknowledge();
        log.info("Ack Invoice: partition={}, offset={}", partition, offset);
    }
}
