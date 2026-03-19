package dev.vality.exporter.businessmetrics.topology;

import dev.vality.exporter.businessmetrics.converter.withdrawal.WithdrawalEventConverterHandler;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(
        prefix = "metrics.topology.withdrawal",
        name = "enabled",
        havingValue = "true"
)
public class WithdrawalMetricsTopology implements MetricsTopology {

    private final Serde<SinkEvent> sinkEventSerde;
    private final Serde<WithdrawalEvent> withdrawalEventSerde;
    private final WithdrawalEventConverterHandler withdrawalEventConverterHandler;
    private final MachineEventParser<TimestampedChange> parser;
    private final MetricsAggregator metricsAggregator;

    private static final String WITHDRAWAL_STARTED_STORE = "withdrawal-started-store";
    private static final String WITHDRAWAL_ROUTE_STORE = "withdrawal-route-store";
    private static final String WITHDRAWAL_STATUS_STORE = "withdrawal-status-store";

    @Value("${spring.kafka.topics.withdrawal}")
    private String withdrawalTopic;

    public void build(StreamsBuilder streamsBuilder) {
        log.info("Start building withdrawal topology");
        KStream<String, SinkEvent> source =
                streamsBuilder.stream(
                                withdrawalTopic,
                                Consumed.with(Serdes.String(), sinkEventSerde)
                                        .withTimestampExtractor(new SinkEventTimestampExtractor()))
                        .peek((key, value) ->
                                log.debug("Source event received. key={}, value={}", key, value));


        KStream<String, WithdrawalEvent> withdrawalEvents = source
                .flatMapValues(sinkEvent -> {
                    try {
                        MachineEvent machineEvent = sinkEvent.getEvent();
                        TimestampedChange change = parser.parse(machineEvent);
                        List<WithdrawalEvent> events = withdrawalEventConverterHandler.handle(machineEvent, change);
                        if (events.isEmpty()) {
                            log.debug("No withdrawal events produced for source id={}", machineEvent.getSourceId());
                        }
                        return events;
                    } catch (Exception ex) {
                        log.warn("Skip sink event due to parsing/conversion error: {}", ex.getMessage(), ex);
                        return List.of();
                    }
                })
                .peek((withdrawalId, event) ->
                        log.trace("Withdrawal event produced. withdrawalId={}, event={}", withdrawalId, event));

        KStream<String, WithdrawalEvent> withdrawals = buildFullWithdrawal(withdrawalEvents);
        for (Duration window : MetricsWindows.WINDOWS) {
            metricsAggregator.aggregateWithdrawals(withdrawals, window);
        }
        metricsAggregator.aggregateTodayWithdrawals(withdrawals);
    }

    private KStream<String, WithdrawalEvent> buildFullWithdrawal(KStream<String, WithdrawalEvent> withdrawalEvents) {

        KStream<String, WithdrawalEvent> startedEvents = withdrawalEvents
                .filter((withdrawalId, event) ->
                        event.getAmount() > 0 && event.getWalletId() != null
                )
                .peek((withdrawalId, event) ->
                        log.trace("Started event selected. withdrawalId={}, amount={}, walletId={}",
                                withdrawalId, event.getAmount(), event.getWalletId()));

        KStream<String, WithdrawalEvent> routeEvents = withdrawalEvents
                .filter((withdrawalId, event) ->
                        event.getProviderId() != 0 && event.getTerminalId() != 0
                                && (event.getAmount() == 0 || event.getWalletId() == null)
                )
                .peek((withdrawalId, event) ->
                        log.trace("Route event selected. withdrawalId={}, providerId={}, terminalId={}",
                                withdrawalId, event.getProviderId(), event.getTerminalId()));

        KStream<String, WithdrawalEvent> statusEvents = withdrawalEvents
                .filter((withdrawalId, event) ->
                        event.getStatus() != null && event.getAmount() == 0
                )
                .peek((withdrawalId, event) ->
                        log.trace("Status event selected. withdrawalId={}, status={}",
                                withdrawalId, event.getStatus()));
        KTable<String, WithdrawalEvent> startedTable = toKTable(startedEvents, WITHDRAWAL_STARTED_STORE);
        KTable<String, WithdrawalEvent> routeTable = toKTable(routeEvents, WITHDRAWAL_ROUTE_STORE);
        KTable<String, WithdrawalEvent> statusTable = toKTable(statusEvents, WITHDRAWAL_STATUS_STORE);

        KTable<String, WithdrawalEvent> fullWithdrawalTable = startedTable
                .leftJoin(routeTable, this::mergeRoute)
                .leftJoin(statusTable, this::mergeStatus);
        return fullWithdrawalTable.toStream()
                .filter((withdrawalId, withdrawalEvent) -> isFullWithdrawalEvent(withdrawalEvent));
    }

    private KTable<String, WithdrawalEvent> toKTable(
            KStream<String, WithdrawalEvent> source,
            String storeName
    ) {
        return source
                .selectKey((withdrawalId, withdrawalEvent) -> withdrawalEvent.getWithdrawalId())
                .groupByKey(Grouped.with(Serdes.String(), withdrawalEventSerde))
                .reduce((oldEvent, newEvent) -> newEvent, Materialized.as(storeName));
    }


    private WithdrawalEvent mergeRoute(WithdrawalEvent event, WithdrawalEvent route) {
        if (route != null) {
            if (route.getProviderId() != 0) {
                event.setProviderId(route.getProviderId());
            }
            if (route.getTerminalId() != 0) {
                event.setTerminalId(route.getTerminalId());
            }
        }
        return event;
    }

    private WithdrawalEvent mergeStatus(WithdrawalEvent event, WithdrawalEvent status) {
        if (status != null) {
            if (status.getStatus() != null) {
                event.setStatus(status.getStatus());
            }
            if (status.getCreatedAt() != null) {
                event.setCreatedAt(status.getCreatedAt());
            }
        }
        return event;
    }

    private static boolean isFullWithdrawalEvent(WithdrawalEvent withdrawalEvent) {
        return withdrawalEvent != null
                && withdrawalEvent.getTerminalId() != 0
                && withdrawalEvent.getProviderId() != 0
                && withdrawalEvent.getWalletId() != null
                && withdrawalEvent.getCurrencyCode() != null
                && withdrawalEvent.getAmount() != 0
                && withdrawalEvent.getStatus() != null;
    }
}
