package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.model.CustomTag;
import dev.vality.exporter.businessmetrics.model.Metric;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
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
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsService {
    private final PaymentMetricsStore paymentMetricsStore;
    private final MeterRegistry registry;

    private MultiGauge paymentsCount;
    private MultiGauge paymentsAmount;

    @PostConstruct
    void init() {
        paymentsCount = MultiGauge.builder(Metric.PAYMENTS_STATUS_COUNT.getName())
                .description(Metric.PAYMENTS_STATUS_COUNT.getDescription())
                .register(registry);

        paymentsAmount = MultiGauge.builder(Metric.PAYMENTS_AMOUNT.getName())
                .description(Metric.PAYMENTS_AMOUNT.getDescription())
                .register(registry);
    }

    @Value("${metrics.ttl.cleaner.enabled}")
    private boolean enabledClean;

    @Scheduled(fixedDelayString = "${metrics.export-ms}")
    public void export() {
        log.debug("Start export metrics");
        if (enabledClean) {
            cleanOldMetrics();
        }
        var rowsCount = new ArrayList<MultiGauge.Row<?>>();
        var rowsAmount = new ArrayList<MultiGauge.Row<?>>();

        paymentMetricsStore.store().forEach((key, aggregation) -> {
            Tags tags = getTags(key);
            rowsCount.add(
                    MultiGauge.Row.of(tags, aggregation.getCount())
            );
            rowsAmount.add(
                    MultiGauge.Row.of(tags, aggregation.getAmount())
            );
        });

        paymentsCount.register(rowsCount, true);
        paymentsAmount.register(rowsAmount, true);
        log.info("Metrics export: paymentMetricsStore size={}, paymentsCount rows={}, paymentsAmount rows={}",
                paymentMetricsStore.store().size(), rowsCount.size(), rowsAmount.size());
    }

    @Value("${metrics.ttl.seconds}")
    private long defaultTtlSeconds;

    @Value("${metrics.ttl.min-lifetime-seconds}")
    private long minLifetimeSeconds;

    private void cleanOldMetrics() {
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
