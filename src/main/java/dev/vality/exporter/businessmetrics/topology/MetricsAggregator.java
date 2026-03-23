package dev.vality.exporter.businessmetrics.topology;

import dev.vality.exporter.businessmetrics.spec.AggregationSpec;
import dev.vality.exporter.businessmetrics.model.MetricsStore;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.function.Function;

@Component
@RequiredArgsConstructor
public class MetricsAggregator {

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
                spec.timestampExtractor(),
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
            Function<A, Instant> timestampExtractor,
            MetricsStore<K, A> store
    ) {
        stream
                .groupBy(
                        (key, event) -> keyExtractor.apply(event),
                        Grouped.with(keySerde, eventSerde)
                )
                .windowedBy(TimeWindows.ofSizeAndGrace(window, Duration.ofMinutes(5)))
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
                                extractDate(timestampExtractor.apply(agg)),
                                agg
                        )
                );
    }

    public <K, V, A> void aggregateToday(
            KStream<String, V> stream,
            AggregationSpec<K, V, A> spec
    ) {
        aggregateToday(
                stream,
                spec.keySerde(),
                spec.eventSerde(),
                spec.aggSerde(),
                spec.initializer(),
                spec.aggregator(),
                spec.keyExtractor(),
                spec.timestampExtractor(),
                spec.store()
        );
    }

    private <K, V, A> void aggregateToday(
            KStream<String, V> stream,
            Serde<K> keySerde,
            Serde<V> eventSerde,
            Serde<A> aggSerde,
            Initializer<A> initializer,
            Aggregator<K, V, A> aggregator,
            Function<V, K> keyExtractor,
            Function<A, Instant> timestampExtractor,
            MetricsStore<K, A> store
    ) {
        stream
                .groupBy(
                        (key, event) -> keyExtractor.apply(event),
                        Grouped.with(keySerde, eventSerde)
                )
                .aggregate(
                        initializer,
                        aggregator,
                        Materialized.with(keySerde, aggSerde)
                )
                .toStream()
                .foreach((key, agg) ->
                        store.put(
                                key,
                                "today",
                                extractDate(timestampExtractor.apply(agg)),
                                agg
                        )
                );
    }

    private LocalDate extractDate(Instant instant) {
        return instant.atZone(ZoneId.of("Europe/Moscow")).toLocalDate();
    }

}
