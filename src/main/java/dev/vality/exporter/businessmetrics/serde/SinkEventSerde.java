package dev.vality.exporter.businessmetrics.serde;

import dev.vality.machinegun.eventsink.SinkEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class SinkEventSerde implements Serde<SinkEvent> {

    @Override
    public Serializer<SinkEvent> serializer() {
        return new SinkEventSerializer();
    }

    @Override
    public Deserializer<SinkEvent> deserializer() {
        return new SinkEventDeserializer();
    }
}


