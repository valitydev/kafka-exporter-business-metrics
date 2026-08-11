package dev.vality.exporter.businessmetrics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.ZoneId;

@Getter
@Setter
@ConfigurationProperties(prefix = "exporter.metrics")
public class MetricsProperties {

    private Long refreshDelayMs;
    private Long transactionLookbackSec;
    private ZoneId timezone;
}
