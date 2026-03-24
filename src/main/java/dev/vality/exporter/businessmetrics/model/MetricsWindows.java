package dev.vality.exporter.businessmetrics.model;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Component
public class MetricsWindows {

    public static final List<Duration> WINDOWS = List.of(
            Duration.ofMinutes(5),
            Duration.ofMinutes(15),
            Duration.ofMinutes(30),
            Duration.ofHours(1),
            Duration.ofHours(3),
            Duration.ofHours(6),
            Duration.ofHours(12),
            Duration.ofHours(24)
    );

    public static final Map<String, Long> WINDOW_TTL_SECONDS = Map.of(
            "5m", 300L,
            "15m", 900L,
            "30m", 1800L,
            "1h", 3600L,
            "3h", 10800L,
            "6h", 21600L,
            "12h", 43200L,
            "24h", 86400L
    );

    public static String tag(Duration window) {
        if (window.toHours() < 1) {
            return window.toMinutes() + "m";
        }
        return window.toHours() + "h";
    }
}
