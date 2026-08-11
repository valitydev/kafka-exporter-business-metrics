package dev.vality.exporter.businessmetrics.kafka.listener;

import dev.vality.exporter.businessmetrics.service.WithdrawalEventService;
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
public class WithdrawalEventListener {

    private final WithdrawalEventService withdrawalEventService;

    @KafkaListener(
            autoStartup = "${kafka.topics.withdrawal.enabled}",
            topics = "${kafka.topics.withdrawal.id}",
            containerFactory = "withdrawalListenerContainerFactory")
    public void listen(
            List<SinkEvent> batch,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) int offset,
            Acknowledgment ack) {
        log.info("Listening Withdrawal: partition={}, offset={}, batch.size()={}", partition, offset, batch.size());
        withdrawalEventService.handleEvents(batch.stream().map(SinkEvent::getEvent).collect(toList()));
        ack.acknowledge();
        log.info("Ack Withdrawal: partition={}, offset={}", partition, offset);
    }
}
