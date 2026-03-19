package dev.vality.exporter.businessmetrics.topology;

import org.apache.kafka.streams.StreamsBuilder;

public interface MetricsTopology {
    void build(StreamsBuilder streamsBuilder);
}
