package dev.vality.exporter.businessmetrics.config;

import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.geck.serializer.Geck;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import dev.vality.sink.common.parser.impl.PaymentEventPayloadMachineEventParser;
import dev.vality.sink.common.serialization.BinaryDeserializer;
import dev.vality.sink.common.serialization.impl.AbstractThriftBinaryDeserializer;
import dev.vality.sink.common.serialization.impl.PaymentEventPayloadDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@SuppressWarnings("LineLength")
public class SerializationConfig {

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
    public BinaryDeserializer<dev.vality.fistful.source.Event> sourceEventDataBinaryDeserializer() {
        return new AbstractThriftBinaryDeserializer<>() {
            @Override
            public dev.vality.fistful.source.Event deserialize(byte[] bytes) {
                return Geck.msgPackToTBase(bytes, dev.vality.fistful.source.Event.class);
            }
        };
    }

    @Bean
    public MachineEventParser<dev.vality.fistful.source.Event> sourceEventDataMachineEventParser(
            BinaryDeserializer<dev.vality.fistful.source.Event> sourceEventDataBinaryDeserializer) {
        return new MachineEventParser<>(sourceEventDataBinaryDeserializer);
    }

    @Bean
    public BinaryDeserializer<dev.vality.fistful.withdrawal.Event> withdrawalEventDataBinaryDeserializer() {
        return new AbstractThriftBinaryDeserializer<>() {
            @Override
            public dev.vality.fistful.withdrawal.Event deserialize(byte[] bytes) {
                return Geck.msgPackToTBase(bytes, dev.vality.fistful.withdrawal.Event.class);
            }
        };
    }

    @Bean
    public MachineEventParser<dev.vality.fistful.withdrawal.Event> withdrawalEventDataMachineEventParser(
            BinaryDeserializer<dev.vality.fistful.withdrawal.Event> withdrawalEventDataBinaryDeserializer) {
        return new MachineEventParser<>(withdrawalEventDataBinaryDeserializer);
    }

    @Bean
    public BinaryDeserializer<dev.vality.fistful.withdrawal_session.Event> withdrawalSessionEventDataBinaryDeserializer() {
        return new AbstractThriftBinaryDeserializer<>() {
            @Override
            public dev.vality.fistful.withdrawal_session.Event deserialize(byte[] bytes) {
                return Geck.msgPackToTBase(bytes, dev.vality.fistful.withdrawal_session.Event.class);
            }
        };
    }

    @Bean
    public MachineEventParser<dev.vality.fistful.withdrawal_session.Event> withdrawalSessionEventDataMachineEventParser(
            BinaryDeserializer<dev.vality.fistful.withdrawal_session.Event> withdrawalSessionEventDataBinaryDeserializer) {
        return new MachineEventParser<>(withdrawalSessionEventDataBinaryDeserializer);
    }
}
