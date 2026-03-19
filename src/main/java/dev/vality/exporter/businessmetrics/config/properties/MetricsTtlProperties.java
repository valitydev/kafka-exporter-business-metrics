package dev.vality.exporter.businessmetrics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "metrics.ttl")
@Getter
@Setter
public class MetricsTtlProperties {

    private long seconds;
    private long minLifetimeSeconds;

    private Cleaner cleaner = new Cleaner();

    @Getter
    @Setter
    public static class Cleaner {
        private boolean enabled;
        private long ms;
    }
}
