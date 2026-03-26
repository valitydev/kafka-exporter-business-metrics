package dev.vality.exporter.businessmetrics.topology;

import dev.vality.exporter.businessmetrics.model.MetricsStore;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.spec.AggregationSpec;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.function.Function;

@Component
@RequiredArgsConstructor
public class MetricsAggregator {

    @Value("${metrics.windowing.update-interval-sec}")
    long updateInterval;

    public <K, V, A> void aggregateWindowed(
            KStream<String, V> stream,
            Duration window,
            AggregationSpec<K, V, A> spec
    ) {
        aggregateWindowed(
                stream,
                window,
                spec.keySerde(),
                spec.eventSerde(),
                spec.aggSerde(),
                spec.initializer(),
                spec.aggregator(),
                spec.keyExtractor(),
                spec.store()
        );
    }

    private <K, V, A> void aggregateWindowed(
            KStream<String, V> stream,
            Duration window,
            Serde<K> keySerde,
            Serde<V> eventSerde,
            Serde<A> aggSerde,
            Initializer<A> initializer,
            Aggregator<K, V, A> aggregator,
            Function<V, K> keyExtractor,
            MetricsStore<K, A> store
    ) {
        stream
                .groupBy(
                        (key, event) -> keyExtractor.apply(event),
                        Grouped.with(keySerde, eventSerde)
                )
                .windowedBy(TimeWindows.ofSizeWithNoGrace(window)
                        .advanceBy(Duration.ofSeconds(updateInterval)))
                .aggregate(
                        initializer,
                        aggregator,
                        Materialized.with(keySerde, aggSerde)
                )
                .toStream()
                .foreach((Windowed<K> windowedKey, A agg) ->
                        store.put(
                                windowedKey.key(),
                                MetricsWindows.tag(window),
                                agg
                        )
                );
    }

}
