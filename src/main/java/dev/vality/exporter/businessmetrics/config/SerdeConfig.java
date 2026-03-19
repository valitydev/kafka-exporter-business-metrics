package dev.vality.exporter.businessmetrics.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.exporter.businessmetrics.model.payments.PaymentAggregation;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricKey;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalAggregation;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalMetricKey;
import dev.vality.exporter.businessmetrics.serde.SinkEventSerde;
import dev.vality.exporter.businessmetrics.topology.MetricsTopology;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import dev.vality.sink.common.parser.impl.PaymentEventPayloadMachineEventParser;
import dev.vality.sink.common.serialization.BinaryDeserializer;
import dev.vality.sink.common.serialization.impl.PaymentEventPayloadDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
public class SerdeConfig {

    @Bean
    public Serde<PaymentEvent> paymentEventSerde(ObjectMapper mapper) {
        JsonSerde<PaymentEvent> serde = new JsonSerde<>(PaymentEvent.class, mapper);
        serde.configure(Map.of(), false);
        return serde;
    }

    @Bean
    public Serde<WithdrawalEvent> withdrawalEventSerde(ObjectMapper mapper) {
        JsonSerde<WithdrawalEvent> serde = new JsonSerde<>(WithdrawalEvent.class, mapper);
        serde.configure(Map.of(), false);
        return serde;
    }

    @Bean
    public Serde<SinkEvent> sinkEventSerde() {
        return new SinkEventSerde();
    }

    @Bean
    public Serde<WithdrawalMetricKey> withdrawalMetricKeySerde(ObjectMapper mapper) {
        JsonSerde<WithdrawalMetricKey> serde = new JsonSerde<>(WithdrawalMetricKey.class, mapper);
        serde.configure(Map.of(), true);
        return serde;
    }

    @Bean
    public Serde<PaymentMetricKey> paymentMetricKeySerde(ObjectMapper mapper) {
        JsonSerde<PaymentMetricKey> serde = new JsonSerde<>(PaymentMetricKey.class, mapper);
        serde.configure(Map.of(), true);
        return serde;
    }

    @Bean
    public Serde<WithdrawalAggregation> withdrawalAggregationSerde(ObjectMapper mapper) {
        JsonSerde<WithdrawalAggregation> serde = new JsonSerde<>(WithdrawalAggregation.class, mapper);
        serde.configure(Map.of(), false);
        return serde;
    }

    @Bean
    public Serde<PaymentAggregation> paymentAggregationSerde(ObjectMapper mapper) {
        JsonSerde<PaymentAggregation> serde = new JsonSerde<>(PaymentAggregation.class, mapper);
        serde.configure(Map.of(), false);
        return serde;
    }

    @Bean
    public BinaryDeserializer<EventPayload> paymentEventPayloadDeserializer() {
        return new PaymentEventPayloadDeserializer();
    }

    @Bean
    public MachineEventParser<EventPayload> paymentEventPayloadMachineEventParser(
            BinaryDeserializer<EventPayload> paymentEventPayloadDeserializer) {
        return new PaymentEventPayloadMachineEventParser(paymentEventPayloadDeserializer);
    }

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();
    }

    @Bean
    public Topology topology(
            List<MetricsTopology> topologies,
            StreamsBuilder streamsBuilder
    ) {
        if (topologies.isEmpty()) {
            log.info("No metrics topologies enabled!");
        }
        topologies.forEach(topology -> {
            log.info("Building topology: {}", topology.getClass().getSimpleName());
            topology.build(streamsBuilder);
        });
        return streamsBuilder.build();
    }
}
