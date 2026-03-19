package dev.vality.exporter.businessmetrics.topology;

import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.model.payments.PaymentAggregation;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricKey;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricsStore;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalAggregation;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalMetricKey;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalMetricsStore;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.ZoneId;

@Component
@RequiredArgsConstructor
public class MetricsAggregator {

    private final Serde<PaymentMetricKey> paymentMetricKeySerde;
    private final Serde<PaymentAggregation> paymentAggregationSerde;
    private final Serde<PaymentEvent> paymentEventSerde;
    private final PaymentMetricsStore paymentMetricsStore;
    private final Serde<WithdrawalMetricKey> withdrawalMetricKeySerde;
    private final Serde<WithdrawalAggregation> withdrawalAggregationSerde;
    private final Serde<WithdrawalEvent> withdrawalEventSerde;
    private final WithdrawalMetricsStore withdrawalMetricsStore;

    public void aggregatePayments(
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
                .foreach((windowedKey, agg) ->
                        paymentMetricsStore.put(
                                windowedKey.key(),
                                MetricsWindows.tag(window),
                                agg.getLastUpdated().atZone(ZoneId.of("Europe/Moscow")).toLocalDate(),
                                agg
                        )
                );
    }

    public void aggregateWithdrawals(
            KStream<String, WithdrawalEvent> stream,
            Duration window
    ) {
        stream
                .groupBy((withdrawalId, withdrawalEvent) -> WithdrawalMetricKey.from(withdrawalEvent),
                        Grouped.with(withdrawalMetricKeySerde, withdrawalEventSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(window, Duration.ofMinutes(5))
                )
                .aggregate(
                        WithdrawalAggregation::new,
                        (metricKey, withdrawalEvent, aggregation) -> aggregation.add(withdrawalEvent),
                        Materialized.with(withdrawalMetricKeySerde, withdrawalAggregationSerde))
                .toStream()
                .foreach((windowedKey, agg) ->
                        withdrawalMetricsStore.put(
                                windowedKey.key(),
                                MetricsWindows.tag(window),
                                agg.getLastUpdated().atZone(ZoneId.of("Europe/Moscow")).toLocalDate(),
                                agg
                        )
                );
    }


    public void aggregateTodayPayments(KStream<String, PaymentEvent> stream) {
        stream
                .groupBy(
                        (key, event) -> PaymentMetricKey.from(event),
                        Grouped.with(paymentMetricKeySerde, paymentEventSerde)
                )
                .aggregate(
                        PaymentAggregation::new,
                        (metricKey, event, agg) -> agg.add(event),
                        Materialized.with(paymentMetricKeySerde, paymentAggregationSerde)
                )
                .toStream()
                .foreach((key, agg) ->
                        paymentMetricsStore.put(
                                key,
                                "today",
                                agg.getLastUpdated().atZone(ZoneId.of("Europe/Moscow")).toLocalDate(),
                                agg
                        )
                );
    }

    public void aggregateTodayWithdrawals(KStream<String, WithdrawalEvent> stream) {
        stream
                .groupBy(
                        (key, event) -> WithdrawalMetricKey.from(event),
                        Grouped.with(withdrawalMetricKeySerde, withdrawalEventSerde)
                )
                .aggregate(
                        WithdrawalAggregation::new,
                        (metricKey, event, agg) -> agg.add(event),
                        Materialized.with(withdrawalMetricKeySerde, withdrawalAggregationSerde)
                )
                .toStream()
                .foreach((key, agg) ->
                        withdrawalMetricsStore.put(
                                key,
                                "today",
                                agg.getLastUpdated().atZone(ZoneId.of("Europe/Moscow")).toLocalDate(),
                                agg
                        )
                );
    }

}
