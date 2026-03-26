package dev.vality.exporter.businessmetrics.topology;

import dev.vality.exporter.businessmetrics.converter.withdrawal.WithdrawalEventConverterHandler;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.exporter.businessmetrics.spec.WithdrawalAggregationSpec;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(
        prefix = "metrics.topology.withdrawal",
        name = "enabled",
        havingValue = "true"
)
public class WithdrawalMetricsTopology extends AbstractMetricsTopology<WithdrawalEvent> {

    private final Serde<SinkEvent> sinkEventSerde;
    private final Serde<WithdrawalEvent> withdrawalEventSerde;
    private final WithdrawalAggregationSpec withdrawalAggregationSpec;
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

        KStream<String, WithdrawalEvent> withdrawals = buildFull(withdrawalEvents);
        metricsAggregator.aggregateSliding(withdrawals, withdrawalAggregationSpec.create());

    }

    @Override
    protected String getStartedStore() {
        return WITHDRAWAL_STARTED_STORE;
    }

    @Override
    protected String getRouteStore() {
        return WITHDRAWAL_ROUTE_STORE;
    }

    @Override
    protected String getStatusStore() {
        return WITHDRAWAL_STATUS_STORE;
    }

    @Override
    protected boolean isStarted(WithdrawalEvent event) {
        return event.getAmount() > 0 && event.getWalletId() != null;
    }

    @Override
    protected boolean isRoute(WithdrawalEvent event) {
        return event.getProviderId() != 0 && event.getTerminalId() != 0
                && (event.getAmount() == 0 || event.getWalletId() == null);
    }

    @Override
    protected boolean isStatus(WithdrawalEvent event) {
        return event.getStatus() != null && event.getAmount() == 0;
    }

    @Override
    protected boolean isFull(WithdrawalEvent event) {
        return event != null
                && event.getTerminalId() != 0
                && event.getProviderId() != 0
                && event.getWalletId() != null
                && event.getCurrencyCode() != null
                && event.getAmount() != 0
                && event.getStatus() != null;
    }

    @Override
    protected WithdrawalEvent mergeRoute(WithdrawalEvent event, WithdrawalEvent route) {
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

    @Override
    protected WithdrawalEvent mergeStatus(WithdrawalEvent event, WithdrawalEvent status) {
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

    @Override
    protected Serde<WithdrawalEvent> getSerde() {
        return withdrawalEventSerde;
    }

    @Override
    protected String getKey(WithdrawalEvent event) {
        return event.getWithdrawalId();
    }
}
