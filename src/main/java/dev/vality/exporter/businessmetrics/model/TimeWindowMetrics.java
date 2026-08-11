package dev.vality.exporter.businessmetrics.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.EnumMap;
import java.util.Map;

@Data
@NoArgsConstructor
public class TimeWindowMetrics {

    private Map<MetricWindow, MetricValues> values =
            new EnumMap<>(MetricWindow.class);

    public MetricValues get(MetricWindow window) {
        return values.get(window);
    }

    public void put(
            MetricWindow window,
            Long count,
            Long amount
    ) {
        values.put(
                window,
                new MetricValues(count, amount)
        );
    }
}
