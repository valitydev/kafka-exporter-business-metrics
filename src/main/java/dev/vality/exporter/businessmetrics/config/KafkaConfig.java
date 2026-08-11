package dev.vality.exporter.businessmetrics.config;

import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.exporter.businessmetrics.config.properties.KafkaConsumerProperties;
import dev.vality.exporter.businessmetrics.kafka.serde.SinkEventDeserializer;
import dev.vality.kafka.common.util.ExponentialBackOffDefaultErrorHandlerFactory;
import dev.vality.machinegun.eventsink.SinkEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
//import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
@SuppressWarnings("LineLength")
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;
    private final KafkaConsumerProperties kafkaConsumerProperties;

    @Bean
    public Map<String, Object> consumerConfigs() {
        return createConsumerConfig();
    }

    private Map<String, Object> createConsumerConfig() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SinkEventDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerProperties.getGroupId());
        return props;
    }

    @Bean
    public ConsumerFactory<String, SinkEvent> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public ConsumerFactory<String, HistoricalCommit> consumerDominantFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, SinkEvent>> invoicingListenerContainerFactory(
            ConsumerFactory<String, SinkEvent> consumerFactory) {
        return createConcurrentFactory(consumerFactory, kafkaConsumerProperties.getInvoicingConcurrency());
    }


    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, SinkEvent>> withdrawalListenerContainerFactory(
            ConsumerFactory<String, SinkEvent> consumerFactory) {
        return createConcurrentFactory(consumerFactory, kafkaConsumerProperties.getWithdrawalConcurrency());
    }

    private <T> KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, T>> createConcurrentFactory(
            ConsumerFactory<String, T> consumerFactory, int threadsNumber) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        initFactory(consumerFactory, threadsNumber, factory);
        return factory;
    }

    private <T> void initFactory(ConsumerFactory<String, T> consumerFactory,
                                 int threadsNumber,
                                 ConcurrentKafkaListenerContainerFactory<String, T> factory) {
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(ExponentialBackOffDefaultErrorHandlerFactory.create());
        factory.setConcurrency(threadsNumber);
    }
}
