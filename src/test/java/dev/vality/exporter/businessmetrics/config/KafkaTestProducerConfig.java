package dev.vality.exporter.businessmetrics.config;

import dev.vality.exporter.businessmetrics.serde.SinkEventSerializer;
import dev.vality.machinegun.eventsink.SinkEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@TestConfiguration
public class KafkaTestProducerConfig {

    @Bean
    public ProducerFactory<String, SinkEvent> sinkEventProducerFactory(
            KafkaProperties kafkaProperties) {

        Map<String, Object> props =
                kafkaProperties.buildProducerProperties();

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                SinkEventSerializer.class
        );

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, SinkEvent> sinkEventKafkaTemplate(
            ProducerFactory<String, SinkEvent> producerFactory) {

        return new KafkaTemplate<>(producerFactory);
    }
}
