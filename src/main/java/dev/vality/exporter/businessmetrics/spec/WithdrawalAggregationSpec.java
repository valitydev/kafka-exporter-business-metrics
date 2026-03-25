package dev.vality.exporter.businessmetrics.spec;

import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalAggregation;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalMetricKey;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalMetricsStore;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class WithdrawalAggregationSpec {

    private final Serde<WithdrawalMetricKey> withdrawalMetricKeySerde;
    private final Serde<WithdrawalAggregation> withdrawalAggregationSerde;
    private final Serde<WithdrawalEvent> withdrawalEventSerde;
    private final WithdrawalMetricsStore withdrawalMetricsStore;

    public AggregationSpec<WithdrawalMetricKey, WithdrawalEvent, WithdrawalAggregation> create() {
        return new AggregationSpec<>(
                withdrawalMetricKeySerde,
                withdrawalEventSerde,
                withdrawalAggregationSerde,
                WithdrawalAggregation::new,
                (key, event, agg) -> agg.add(event),
                WithdrawalMetricKey::from,
                WithdrawalAggregation::getLastUpdated,
                withdrawalMetricsStore
        );
    }
}
