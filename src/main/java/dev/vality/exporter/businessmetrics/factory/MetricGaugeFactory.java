package dev.vality.exporter.businessmetrics.factory;

import dev.vality.exporter.businessmetrics.model.Metric;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MetricGaugeFactory {

    private final MeterRegistry meterRegistry;

    public MultiGauge create(Metric metric) {
        return MultiGauge.builder(metric.getName())
                .description(metric.getDescription())
                .register(meterRegistry);
    }
}
