package dev.vality.exporter.businessmetrics.topology;

import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.exporter.businessmetrics.converter.InvoiceEventConverterHandler;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
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
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentMetricsTopology {

    private final Serde<SinkEvent> sinkEventSerde;
    private final Serde<PaymentEvent> paymentEventSerde;
    private final InvoiceEventConverterHandler invoiceEventConverterHandler;
    private final MachineEventParser<EventPayload> parser;
    private final MetricsAggregator metricsAggregator;

    private static final String PAYMENT_STARTED_STORE = "payment-started-store";
    private static final String PAYMENT_ROUTE_STORE = "payment-route-store";
    private static final String PAYMENT_STATUS_STORE = "payment-status-store";

    @Value("${spring.kafka.topics.invoice}")
    private String invoiceTopic;

    public void buildPaymentTopology(StreamsBuilder streamsBuilder) {
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

        KStream<String, PaymentEvent> payments = buildFullPayment(paymentEvents);
        for (Duration window : MetricsWindows.WINDOWS) {
            metricsAggregator.aggregate(payments, window);
        }
    }

    private KStream<String, PaymentEvent> buildFullPayment(KStream<String, PaymentEvent> paymentEvents) {

        KStream<String, PaymentEvent> startedEvents = paymentEvents
                .filter((invoiceId, event) ->
                        event.getAmount() > 0 && event.getShopId() != null
                )
                .peek((invoiceId, event) ->
                        log.trace("Started event selected. invoiceId={}, amount={}, shopId={}",
                                invoiceId, event.getAmount(), event.getShopId()));

        KStream<String, PaymentEvent> routeEvents = paymentEvents
                .filter((invoiceId, event) ->
                        event.getProviderId() != 0 && event.getTerminalId() != 0
                                && (event.getAmount() == 0 || event.getShopId() == null)
                )
                .peek((invoiceId, event) ->
                        log.trace("Route event selected. invoiceId={}, providerId={}, terminalId={}",
                                invoiceId, event.getProviderId(), event.getTerminalId()));

        KStream<String, PaymentEvent> statusEvents = paymentEvents
                .filter((invoiceId, event) ->
                        event.getStatus() != null && event.getAmount() == 0
                )
                .peek((invoiceId, event) ->
                        log.trace("Status event selected. invoiceId={}, status={}",
                                invoiceId, event.getStatus()));
        KTable<String, PaymentEvent> startedTable = toKTable(startedEvents, PAYMENT_STARTED_STORE);
        KTable<String, PaymentEvent> routeTable = toKTable(routeEvents, PAYMENT_ROUTE_STORE);
        KTable<String, PaymentEvent> statusTable = toKTable(statusEvents, PAYMENT_STATUS_STORE);

        KTable<String, PaymentEvent> fullPaymentTable = startedTable
                .leftJoin(routeTable, this::mergeRoute)
                .leftJoin(statusTable, this::mergeStatus);
        return fullPaymentTable.toStream()
                .filter((invoiceId, paymentEvent) -> paymentEvent != null);
    }

    private KTable<String, PaymentEvent> toKTable(
            KStream<String, PaymentEvent> source,
            String storeName
    ) {
        return source
                .selectKey((invoiceId, paymentEvent) -> paymentEvent.getInvoiceId())
                .groupByKey(Grouped.with(Serdes.String(), paymentEventSerde))
                .reduce((oldEvent, newEvent) -> newEvent, Materialized.as(storeName));
    }


    private PaymentEvent mergeRoute(PaymentEvent event, PaymentEvent route) {
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

    private PaymentEvent mergeStatus(PaymentEvent event, PaymentEvent status) {
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
}
