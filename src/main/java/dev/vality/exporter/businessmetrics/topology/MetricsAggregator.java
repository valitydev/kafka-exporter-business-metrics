package dev.vality.exporter.businessmetrics.topology;

import dev.vality.exporter.businessmetrics.model.MetricsStore;
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
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.function.Function;

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
        aggregateWithWindow(
                stream,
                window,
                paymentMetricKeySerde,
                paymentEventSerde,
                paymentAggregationSerde,
                PaymentAggregation::new,
                (key, event, agg) -> agg.add(event),
                PaymentMetricKey::from,
                paymentMetricsStore
        );
    }

    public void aggregateWithdrawals(
            KStream<String, WithdrawalEvent> stream,
            Duration window
    ) {
        aggregateWithWindow(
                stream,
                window,
                withdrawalMetricKeySerde,
                withdrawalEventSerde,
                withdrawalAggregationSerde,
                WithdrawalAggregation::new,
                (key, event, agg) -> agg.add(event),
                WithdrawalMetricKey::from,
                withdrawalMetricsStore
        );
    }


    public void aggregateTodayPayments(KStream<String, PaymentEvent> stream) {
        aggregateToday(
                stream,
                paymentMetricKeySerde,
                paymentEventSerde,
                paymentAggregationSerde,
                PaymentAggregation::new,
                (key, event, agg) -> agg.add(event),
                PaymentMetricKey::from,
                paymentMetricsStore
        );
    }

    public void aggregateTodayWithdrawals(KStream<String, WithdrawalEvent> stream) {
        aggregateToday(
                stream,
                withdrawalMetricKeySerde,
                withdrawalEventSerde,
                withdrawalAggregationSerde,
                WithdrawalAggregation::new,
                (key, event, agg) -> agg.add(event),
                WithdrawalMetricKey::from,
                withdrawalMetricsStore
        );
    }

    private <K, V, A> void aggregateWithWindow(
            KStream<String, V> stream,
            Duration window,
            Serde<K> keySerde,
            Serde<V> eventSerde,
            Serde<A> aggSerde,
            Initializer<A> initializer,
            Aggregator<K, V, A> aggregator,
            Function<V, K> keyExtractor,
            MetricsStore<K, A> store
    ) {
        stream
                .groupBy(
                        (key, event) -> keyExtractor.apply(event),
                        Grouped.with(keySerde, eventSerde)
                )
                .windowedBy(TimeWindows.ofSizeAndGrace(window, Duration.ofMinutes(5)))
                .aggregate(
                        initializer,
                        aggregator,
                        Materialized.with(keySerde, aggSerde)
                )
                .toStream()
                .foreach((Windowed<K> windowedKey, A agg) ->
                        store.put(
                                windowedKey.key(),
                                MetricsWindows.tag(window),
                                extractDate(agg),
                                agg
                        )
                );
    }

    private <K, V, A> void aggregateToday(
            KStream<String, V> stream,
            Serde<K> keySerde,
            Serde<V> eventSerde,
            Serde<A> aggSerde,
            Initializer<A> initializer,
            Aggregator<K, V, A> aggregator,
            Function<V, K> keyExtractor,
            MetricsStore<K, A> store
    ) {
        stream
                .groupBy(
                        (key, event) -> keyExtractor.apply(event),
                        Grouped.with(keySerde, eventSerde)
                )
                .aggregate(
                        initializer,
                        aggregator,
                        Materialized.with(keySerde, aggSerde)
                )
                .toStream()
                .foreach((key, agg) ->
                        store.put(
                                key,
                                "today",
                                extractDate(agg),
                                agg
                        )
                );
    }

    private <A> LocalDate extractDate(A agg) {
        Instant lastUpdated;

        if (agg instanceof PaymentAggregation paymentAggregation) {
            lastUpdated = paymentAggregation.getLastUpdated();
        } else if (agg instanceof WithdrawalAggregation withdrawalAggregation) {
            lastUpdated = withdrawalAggregation.getLastUpdated();
        } else {
            throw new IllegalArgumentException("Unknown aggregation type");
        }

        return lastUpdated.atZone(ZoneId.of("Europe/Moscow")).toLocalDate();
    }

}
