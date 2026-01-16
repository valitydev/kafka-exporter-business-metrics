package dev.vality.exporter.businessmetrics.topology;

import dev.vality.machinegun.eventsink.SinkEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class SinkEventTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        SinkEvent event = (SinkEvent) record.value();
        return Instant.parse(event.getEvent().getCreatedAt()).toEpochMilli();
    }
}
