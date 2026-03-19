package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.model.MetricsBinding;
import dev.vality.exporter.businessmetrics.model.MetricsBindingRegistry;
import dev.vality.exporter.businessmetrics.config.properties.MetricsTtlProperties;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsService {

    private final MetricsBindingRegistry metricsBindingRegistry;
    private final MeterRegistry registry;
    private final MetricsTtlProperties props;

    private List<MetricsBinding<?, ?>> configs;

    @PostConstruct
    void init() {
        configs = metricsBindingRegistry.all();
        registerScrapes();
    }

    private void registerScrapes() {
        configs.forEach(this::register);
    }

    private <K, A> void register(MetricsBinding<K, A> config) {

        registry.gauge(config.countScrapeName(), config.store(), store -> {
            updateMultiGauge(
                    config.gaugeCount(),
                    config.store(),
                    config.tagsExtractor(),
                    config.countExtractor()
            );
            return config.store().size();
        });

        registry.gauge(config.amountScrapeName(), config.store(), store -> {
            updateMultiGauge(
                    config.gaugeAmount(),
                    config.store(),
                    config.tagsExtractor(),
                    config.amountExtractor()
            );
            return config.store().size();
        });
    }

    private <K, A> void updateMultiGauge(MultiGauge gauge, Map<K, A> store,
                                         Function<K, Tags> tagsExtractor,
                                         ToDoubleFunction<A> valueExtractor) {
        List<MultiGauge.Row<Number>> rows = store.entrySet().stream()
                .map(entry -> MultiGauge.Row.of(
                        tagsExtractor.apply(entry.getKey()),
                        valueExtractor.applyAsDouble(entry.getValue()))).toList();
        gauge.register(rows, true);
    }

    @Scheduled(fixedDelayString = "${metrics.ttl.cleaner.ms}")
    private void cleanOldMetrics() {
        if (!props.getCleaner().isEnabled()) {
            return;
        }
        log.debug("Start clean old metrics");
        configs.forEach(this::cleanStore);

    }

    private <K, A> void cleanStore(MetricsBinding<K, A> config) {

        Instant now = Instant.now();

        config.store().forEach((key, aggregation) -> {

            Instant lastUpdated = config.lastUpdatedExtractor().apply(aggregation);

            if (lastUpdated.plusSeconds(props.getMinLifetimeSeconds()).isAfter(now)) {
                return;
            }

            long ttl = MetricsWindows.WINDOW_TTL_SECONDS
                    .getOrDefault(config.windowExtractor().apply(key), props.getSeconds());

            if (lastUpdated.plusSeconds(ttl).isBefore(now)) {

                Tags tags = config.tagsExtractor().apply(key);

                for (String metricName : config.metricNames()) {
                    Meter meter = registry.find(metricName)
                            .tags(tags)
                            .meter();

                    if (meter != null) {
                        registry.remove(meter);
                    }
                }

                config.remover().accept(key);

                log.info("Removed expired metric key={} ttl={}", key, ttl);
            }
        });
    }
}
