package dev.vality.exporter.businessmetrics.topology;

import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.exporter.businessmetrics.converter.payment.InvoiceEventConverterHandler;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.exporter.businessmetrics.spec.PaymentAggregationSpec;
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

import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(
        prefix = "metrics.topology.payment",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class PaymentMetricsTopology extends AbstractMetricsTopology<PaymentEvent> {

    private final Serde<SinkEvent> sinkEventSerde;
    private final Serde<PaymentEvent> paymentEventSerde;
    private final PaymentAggregationSpec paymentAggregationSpec;
    private final InvoiceEventConverterHandler invoiceEventConverterHandler;
    private final MachineEventParser<EventPayload> parser;
    private final MetricsAggregator metricsAggregator;

    private static final String PAYMENT_STARTED_STORE = "payment-started-store";
    private static final String PAYMENT_ROUTE_STORE = "payment-route-store";
    private static final String PAYMENT_STATUS_STORE = "payment-status-store";

    @Value("${spring.kafka.topics.invoice}")
    private String invoiceTopic;

    @Override
    public void build(StreamsBuilder streamsBuilder) {
        log.info("Start building payment topology");
        KStream<String, SinkEvent> source =
                streamsBuilder.stream(
                                invoiceTopic,
                                Consumed.with(Serdes.String(), sinkEventSerde)
                                        .withTimestampExtractor(new SinkEventTimestampExtractor()))
                        .peek((key, value) ->
                                log.debug("Source event received. key={}, value={}", key, value));

        KStream<String, PaymentEvent> paymentEvents = source
                .flatMapValues(sinkEvent -> {
                    try {
                        MachineEvent machineEvent = sinkEvent.getEvent();
                        EventPayload payload = parser.parse(machineEvent);
                        List<PaymentEvent> events = invoiceEventConverterHandler.handle(machineEvent, payload);
                        if (events.isEmpty()) {
                            log.debug("No payment events produced for source id={}", machineEvent.getSourceId());
                        }
                        return events;
                    } catch (Exception ex) {
                        log.warn("Skip sink event due to parsing/conversion error: {}", ex.getMessage(), ex);
                        return List.of();
                    }
                })
                .peek((invoiceId, event) ->
                        log.trace("Payment event produced. invoiceId={}, event={}", invoiceId, event));

        KStream<String, PaymentEvent> fullPayments = buildFull(paymentEvents);
        for (Duration window : MetricsWindows.WINDOWS) {
            metricsAggregator.aggregateWindowed(fullPayments, window, paymentAggregationSpec.create());
        }
        metricsAggregator.aggregateToday(fullPayments, paymentAggregationSpec.create());
    }

    @Override
    protected String getStartedStore() {
        return PAYMENT_STARTED_STORE;
    }

    @Override
    protected String getRouteStore() {
        return PAYMENT_ROUTE_STORE;
    }

    @Override
    protected String getStatusStore() {
        return PAYMENT_STATUS_STORE;
    }

    @Override
    protected Serde<PaymentEvent> getSerde() {
        return paymentEventSerde;
    }

    @Override
    protected String getKey(PaymentEvent event) {
        return event.getInvoiceId();
    }

    @Override
    protected boolean isStarted(PaymentEvent event) {
        return event.getAmount() > 0 && event.getShopId() != null;
    }

    @Override
    protected boolean isRoute(PaymentEvent event) {
        return event.getProviderId() != 0
                && event.getTerminalId() != 0;
    }

    @Override
    protected boolean isStatus(PaymentEvent event) {
        return event.getStatus() != null;
    }

    @Override
    protected PaymentEvent mergeRoute(PaymentEvent base, PaymentEvent route) {
        if (route != null) {
            if (route.getProviderId() != 0) {
                base.setProviderId(route.getProviderId());
            }
            if (route.getTerminalId() != 0) {
                base.setTerminalId(route.getTerminalId());
            }
        }
        return base;
    }

    @Override
    protected PaymentEvent mergeStatus(PaymentEvent base, PaymentEvent status) {
        if (status != null) {
            if (status.getStatus() != null) {
                base.setStatus(status.getStatus());
            }
            if (status.getCreatedAt() != null) {
                base.setCreatedAt(status.getCreatedAt());
            }
        }
        return base;
    }

    @Override
    protected boolean isFull(PaymentEvent event) {
        return event != null
                && event.getTerminalId() != 0
                && event.getProviderId() != 0
                && event.getShopId() != null
                && event.getCurrencyCode() != null
                && event.getAmount() != 0
                && event.getStatus() != null;
    }
}
