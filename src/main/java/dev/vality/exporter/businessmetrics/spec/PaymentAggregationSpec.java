package dev.vality.exporter.businessmetrics.spec;

import dev.vality.exporter.businessmetrics.model.payments.PaymentAggregation;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricKey;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricsStore;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PaymentAggregationSpec {

    private final Serde<PaymentMetricKey> paymentMetricKeySerde;
    private final Serde<PaymentAggregation> paymentAggregationSerde;
    private final Serde<PaymentEvent> paymentEventSerde;
    private final PaymentMetricsStore paymentMetricsStore;

    public AggregationSpec<PaymentMetricKey, PaymentEvent, PaymentAggregation> create() {
        return new AggregationSpec<>(
                paymentMetricKeySerde,
                paymentEventSerde,
                paymentAggregationSerde,
                PaymentAggregation::new,
                (key, event, agg) -> agg.add(event),
                PaymentMetricKey::from,
                PaymentAggregation::getLastUpdated,
                paymentMetricsStore
        );
    }
}
