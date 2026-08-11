package dev.vality.exporter.businessmetrics.serde;

import dev.vality.machinegun.eventsink.SinkEvent;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.transport.TTransportException;

public class SinkEventSerializer implements Serializer<SinkEvent> {

    private final TSerializer serializer;

    public SinkEventSerializer() {
        try {
            this.serializer = new TSerializer();
        } catch (TTransportException e) {
            throw new IllegalStateException(
                    "Failed to create Thrift serializer",
                    e
            );
        }
    }

    @Override
    public byte[] serialize(String topic, SinkEvent data) {
        if (data == null) {
            return null;
        }

        try {
            return serializer.serialize(data);
        } catch (TException e) {
            throw new SerializationException(
                    "Failed to serialize SinkEvent",
                    e
            );
        }
    }

    @Override
    public void close() {
        // nothing to close
    }
}
