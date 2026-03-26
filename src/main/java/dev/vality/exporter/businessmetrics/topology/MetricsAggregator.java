package dev.vality.exporter.businessmetrics.topology;

import dev.vality.exporter.businessmetrics.model.MetricsStore;
import dev.vality.exporter.businessmetrics.spec.AggregationSpec;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.function.Function;

@Component
@RequiredArgsConstructor
public class MetricsAggregator {

    private static final Duration SLIDING_WINDOW_24H = Duration.ofHours(24);

    public <K, V, A> void aggregateSliding(
            KStream<String, V> stream,
            AggregationSpec<K, V, A> spec
    ) {
        aggregateSliding(
                stream,
                spec.keySerde(),
                spec.eventSerde(),
                spec.aggSerde(),
                spec.initializer(),
                spec.aggregator(),
                spec.keyExtractor(),
                spec.store()
        );
    }

    private <K, V, A> void aggregateSliding(
            KStream<String, V> stream,
            Serde<K> keySerde,
            Serde<V> eventSerde,
            Serde<A> aggSerde,
            Initializer<A> initializer,
            Aggregator<K, V, A> aggregator,
            Function<V, K> keyExtractor,
            MetricsStore<K, A> store
    ) {
        stream
                .groupBy((key, event) -> keyExtractor.apply(event), Grouped.with(keySerde, eventSerde))
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(SLIDING_WINDOW_24H))
                .aggregate(initializer, aggregator, Materialized.with(keySerde, aggSerde))
                .toStream()
                .foreach((windowedKey, agg) ->
                        store.put(windowedKey.key(), "24h", agg)
                );
    }
}