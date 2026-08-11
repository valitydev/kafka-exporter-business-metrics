package dev.vality.exporter.businessmetrics.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Duration;

@Getter
@RequiredArgsConstructor
public enum MetricWindow {

    M5("5m", Duration.ofMinutes(5)),
    M15("15m", Duration.ofMinutes(15)),
    M30("30m", Duration.ofMinutes(30)),
    H1("1h", Duration.ofHours(1)),
    H3("3h", Duration.ofHours(3)),
    H6("6h", Duration.ofHours(6)),
    H12("12h", Duration.ofHours(12)),
    H24("24h", Duration.ofHours(24)),
    TODAY_MSK("today_msk", null);

    private final String label;
    private final Duration duration;
}
