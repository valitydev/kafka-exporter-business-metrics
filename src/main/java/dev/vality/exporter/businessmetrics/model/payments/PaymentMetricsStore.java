package dev.vality.exporter.businessmetrics.model.payments;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class PaymentMetricsStore {

    public record MetricKey(
            int providerId,
            int terminalId,
            String shopId,
            String currency,
            String status,
            String window
    ) {}

    private final ConcurrentMap<MetricKey, PaymentAggregation> store =
            new ConcurrentHashMap<>();

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
        return Map.copyOf(store);
    }

    public void remove(MetricKey key) {
        store.remove(key);
    }
}
