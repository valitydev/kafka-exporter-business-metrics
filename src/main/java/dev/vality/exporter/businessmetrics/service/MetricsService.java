package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.model.CustomTag;
import dev.vality.exporter.businessmetrics.model.Metric;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.model.payments.PaymentAggregation;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricKey;
import io.micrometer.core.instrument.Meter;
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
        final Instant createdAt;
        Instant lastUpdated;

        GaugeHolder(AtomicLong value) {
            this.value = value;
            this.createdAt = Instant.now();
            this.lastUpdated = this.createdAt;
        }
    }

    private final Map<MetricId, GaugeHolder> gauges = new ConcurrentHashMap<>();

    public void update(PaymentMetricKey key, String duration, PaymentAggregation agg) {
        Tags tags = buildTags(key, duration);
        log.debug(
                "Metric Update {} tags={} count={}",
                Metric.PAYMENTS_STATUS_COUNT.getName(),
                tags,
                agg.getCount()
        );

        updateGauge(Metric.PAYMENTS_STATUS_COUNT.getName(), tags, agg.getCount());
        log.debug(
                "Metric Update {} tags={} amount={}",
                Metric.PAYMENTS_AMOUNT.getName(),
                tags,
                agg.getAmount()
        );
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

    @Value("${metrics.ttl.seconds}")
    private long defaultTtlSeconds;

    @Value("${metrics.ttl.min-lifetime-seconds}")
    private long minLifetimeSeconds;

    @Scheduled(fixedDelayString = "${metrics.ttl.cleaner.ms}")
    public void cleanOldGauges() {
        Instant now = Instant.now();

        gauges.entrySet().removeIf(entry -> {
            MetricId metricId = entry.getKey();
            GaugeHolder holder = entry.getValue();
            if (holder.createdAt.plusSeconds(minLifetimeSeconds).isAfter(now)) {
                return false;
            }
            String duration = metricId.tags.stream()
                    .filter(tag -> tag.getKey().equals(CustomTag.DURATION_TAG))
                    .map(Tag::getValue)
                    .findFirst()
                    .orElse(null);

            long ttl = MetricsWindows.WINDOW_TTL_SECONDS.getOrDefault(duration, defaultTtlSeconds);

            boolean expired = holder.lastUpdated.plusSeconds(ttl).isBefore(now);
            if (expired) {
                Meter meter = registry.find(metricId.name)
                        .tags(metricId.tags)
                        .meter();
                if (meter != null) {
                    registry.remove(meter);
                }
                log.debug("Removing expired Gauge: {}, duration={}, ttl={}s", metricId, duration, ttl);            }
            return expired;
        });
    }
}
