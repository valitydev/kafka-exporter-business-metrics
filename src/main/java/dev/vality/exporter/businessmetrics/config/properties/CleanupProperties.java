package dev.vality.exporter.businessmetrics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "exporter.cleanup")
@Getter
@Setter
public class CleanupProperties {

    private String cron;
    private int retentionDays;
    private int batchSize;
}
