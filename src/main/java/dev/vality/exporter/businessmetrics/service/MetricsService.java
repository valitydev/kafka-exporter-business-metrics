package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.model.CustomTag;
import dev.vality.exporter.businessmetrics.model.Metric;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.model.payments.PaymentAggregation;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricsStore;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsService {
    private final PaymentMetricsStore paymentMetricsStore;
    private final MeterRegistry registry;

    private MultiGauge paymentsCount;
    private MultiGauge paymentsAmount;

    private static final String PAYMENT_COUNT_SCRAPE = "payments_count_scrape";
    private static final String PAYMENT_AMOUNT_SCRAPE = "payments_amount_scrape";

    @PostConstruct
    void init() {
        paymentsCount = MultiGauge.builder(Metric.PAYMENTS_STATUS_COUNT.getName())
                .description(Metric.PAYMENTS_STATUS_COUNT.getDescription())
                .register(registry);

        paymentsAmount = MultiGauge.builder(Metric.PAYMENTS_AMOUNT.getName())
                .description(Metric.PAYMENTS_AMOUNT.getDescription())
                .register(registry);

        registry.gauge(PAYMENT_COUNT_SCRAPE, paymentMetricsStore, store -> {
            updateMultiGauge(paymentsCount, paymentMetricsStore.store(), PaymentAggregation::getCount);
            return paymentMetricsStore.store().size();
        });

        registry.gauge(PAYMENT_AMOUNT_SCRAPE, paymentMetricsStore, store -> {
            updateMultiGauge(paymentsAmount, paymentMetricsStore.store(), PaymentAggregation::getAmount);
            return paymentMetricsStore.store().size();
        });
    }

    public void updateMultiGauge(
            MultiGauge gauge,
            Map<PaymentMetricsStore.MetricKey, PaymentAggregation> store,
            java.util.function.ToDoubleFunction<PaymentAggregation> valueExtractor
    ) {
        List<MultiGauge.Row<Number>> rows = store.entrySet().stream()
                .map(entry ->
                        MultiGauge.Row.of(getTags(entry.getKey()),
                                valueExtractor.applyAsDouble(entry.getValue())))
                .toList();
        gauge.register(rows, true);
    }

    @Value("${metrics.ttl.seconds}")
    private long defaultTtlSeconds;

    @Value("${metrics.ttl.min-lifetime-seconds}")
    private long minLifetimeSeconds;

    @Value("${metrics.ttl.cleaner.enabled}")
    private boolean enabledClean;

    @Scheduled(fixedDelayString = "${metrics.ttl.cleaner.ms}")
    private void cleanOldMetrics() {
        if (enabledClean) {
            log.debug("Start clean old metrics");
            Instant now = Instant.now();

            paymentMetricsStore.store().forEach((key, aggregation) -> {
                if (aggregation.getLastUpdated().plusSeconds(minLifetimeSeconds).isAfter(now)) {
                    return;
                }

                long ttl = MetricsWindows.WINDOW_TTL_SECONDS.getOrDefault(key.window(), defaultTtlSeconds);

                boolean expired = aggregation.getLastUpdated().plusSeconds(ttl).isBefore(now);
                if (expired) {
                    Tags tags = getTags(key);

                    for (String metricName : List.of(
                            Metric.PAYMENTS_STATUS_COUNT.getName(),
                            Metric.PAYMENTS_AMOUNT.getName())) {

                        Meter meter = registry.find(metricName)
                                .tags(tags)
                                .meter();
                        if (meter != null) {
                            registry.remove(meter);
                        }
                    }

                    paymentMetricsStore.remove(key);

                    log.info("Removed expired metric for key={} with TTL={}s", key, ttl);
                }
            });
        }
    }

    private Tags getTags(PaymentMetricsStore.MetricKey key) {
        return Tags.of(
                CustomTag.providerId(String.valueOf(key.providerId())),
                CustomTag.terminalId(String.valueOf(key.terminalId())),
                CustomTag.shopId(key.shopId()),
                CustomTag.currency(key.currency()),
                CustomTag.status(key.status()),
                CustomTag.duration(key.window())
        );
    }
}
