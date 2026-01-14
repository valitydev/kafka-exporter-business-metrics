package dev.vality.exporter.businessmetrics.topology;

import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.model.payments.PaymentAggregation;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricKey;
import dev.vality.exporter.businessmetrics.service.MetricsService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class MetricsAggregator {

    private final Serde<PaymentMetricKey> paymentMetricKeySerde;
    private final Serde<PaymentAggregation> paymentAggregationSerde;
    private final Serde<PaymentEvent> paymentEventSerde;
    private final MetricsService metricsService;

    public void aggregate(
            KStream<String, PaymentEvent> stream,
            Duration window
    ) {
        stream
                .groupBy((invoiceId, paymentEvent) -> PaymentMetricKey.from(paymentEvent),
                        Grouped.with(paymentMetricKeySerde, paymentEventSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(window, Duration.ofMinutes(5))
                )
                .aggregate(
                        PaymentAggregation::new,
                        (metricKey, paymentEvent, aggregation) -> aggregation.add(paymentEvent),
                        Materialized.with(paymentMetricKeySerde, paymentAggregationSerde))
                .toStream()
                .foreach((windowedMetricKey, aggregation) -> {
                            metricsService.update(windowedMetricKey.key(), MetricsWindows.tag(window), aggregation);
                            }
                );
    }
}
