package dev.vality.exporter.businessmetrics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "kafka.consumer")
public class KafkaConsumerProperties {

    private String groupId;
    private int invoicingConcurrency;
    private int withdrawalConcurrency;
}
