package dev.vality.exporter.businessmetrics.spec;

import dev.vality.exporter.businessmetrics.model.MetricsStore;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;

import java.time.Instant;
import java.util.function.Function;

public record AggregationSpec<K, V, A>(
        Serde<K> keySerde,
        Serde<V> eventSerde,
        Serde<A> aggSerde,
        Initializer<A> initializer,
        Aggregator<K, V, A> aggregator,
        Function<V, K> keyExtractor,
        Function<A, Instant> timestampExtractor,
        MetricsStore<K, A> store
) {}
