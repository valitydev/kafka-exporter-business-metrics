package dev.vality.exporter.businessmetrics.model.payments;

import dev.vality.exporter.businessmetrics.model.MetricsStore;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class PaymentMetricsStore implements MetricsStore<PaymentMetricKey, PaymentAggregation> {

    public record MetricKey(
            int providerId,
            int terminalId,
            String shopId,
            String currency,
            String status,
            String window
    ) {
    }

    private final ConcurrentMap<MetricKey, PaymentAggregation> store =
            new ConcurrentHashMap<>();

    @Override
    public void put(
            PaymentMetricKey key,
            String window,
            PaymentAggregation agg
    ) {
        store.put(
                new MetricKey(
                        key.getProviderId(),
                        key.getTerminalId(),
                        key.getShopId(),
                        key.getCurrencyCode(),
                        key.getStatus(),
                        window
                ),
                agg
        );
    }

    public Map<MetricKey, PaymentAggregation> store() {
        return store;
    }

    public void remove(MetricKey key) {
        store.remove(key);
    }
}
