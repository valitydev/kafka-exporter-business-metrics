package dev.vality.exporter.businessmetrics.model.withdrawals;

import dev.vality.exporter.businessmetrics.model.MetricsStore;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class WithdrawalMetricsStore implements MetricsStore<WithdrawalMetricKey, WithdrawalAggregation> {

    public record MetricKey(
            int providerId,
            int terminalId,
            String walletId,
            String currency,
            String status,
            String window,
            LocalDate date
    ) {
    }

    private final ConcurrentMap<MetricKey, WithdrawalAggregation> store =
            new ConcurrentHashMap<>();

    @Override
    public void put(
            WithdrawalMetricKey key,
            String window,
            LocalDate date,
            WithdrawalAggregation agg
    ) {
        store.put(
                new MetricKey(
                        key.getProviderId(),
                        key.getTerminalId(),
                        key.getWalletId(),
                        key.getCurrencyCode(),
                        key.getStatus(),
                        window,
                        date
                ),
                agg
        );
    }

    public Map<MetricKey, WithdrawalAggregation> store() {
        return store;
    }

    public void remove(MetricKey key) {
        store.remove(key);
    }
}
