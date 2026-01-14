package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.model.CustomTag;
import dev.vality.exporter.businessmetrics.model.Metric;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.model.payments.PaymentAggregation;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricKey;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsService {
    private final MeterRegistry registry;

    private record MetricId(String name, Tags tags) {
    }

    private static class GaugeHolder {
        final AtomicLong value;
        Instant lastUpdated;

        GaugeHolder(AtomicLong value) {
            this.value = value;
            this.lastUpdated = Instant.now();
        }
    }

    private final Map<MetricId, GaugeHolder> gauges = new ConcurrentHashMap<>();

    @Value("${metrics.ttl.seconds}")
    private long defaultTtlSeconds;

    public void update(PaymentMetricKey key, String duration, PaymentAggregation agg) {
        Tags tags = buildTags(key, duration);

        updateGauge(Metric.PAYMENTS_STATUS_COUNT.getName(), tags, agg.getCount());
        updateGauge(Metric.PAYMENTS_AMOUNT.getName(), tags, agg.getAmount());
    }

    private Tags buildTags(PaymentMetricKey key, String duration) {
        return Tags.of(
                CustomTag.providerId(String.valueOf(key.getProviderId())),
                CustomTag.terminalId(String.valueOf(key.getTerminalId())),
                CustomTag.shopId(key.getShopId()),
                CustomTag.currency(key.getCurrencyCode()),
                CustomTag.status(key.getStatus()),
                CustomTag.duration(duration)
        );
    }

    private void updateGauge(String metricName, Tags tags, long value) {
        MetricId id = new MetricId(metricName, tags);

        GaugeHolder holder = gauges.computeIfAbsent(id, k -> {
            AtomicLong atomic = new AtomicLong(0);
            registry.gauge(metricName, tags, atomic);
            return new GaugeHolder(atomic);
        });

        holder.value.set(value);
        holder.lastUpdated = Instant.now();
    }

    @Scheduled(fixedDelayString = "${metrics.ttl.cleaner.ms}")
    public void cleanOldGauges() {
        Instant now = Instant.now();

        gauges.entrySet().removeIf(entry -> {
            MetricId metricId = entry.getKey();
            GaugeHolder holder = entry.getValue();

            String duration = metricId.tags.stream()
                    .filter(tag -> tag.getKey().equals(CustomTag.DURATION_TAG))
                    .map(Tag::getValue)
                    .findFirst()
                    .orElse(null);

            long ttl = MetricsWindows.WINDOW_TTL_SECONDS.getOrDefault(duration, defaultTtlSeconds);

            boolean expired = holder.lastUpdated.plusSeconds(ttl).isBefore(now);
            if (expired) {
                log.debug("Removing expired Gauge: {}, duration={}, ttl={}s", metricId, duration, ttl);
            }
            return expired;
        });
    }
}
