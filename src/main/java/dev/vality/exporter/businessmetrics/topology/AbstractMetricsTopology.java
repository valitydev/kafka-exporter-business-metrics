package dev.vality.exporter.businessmetrics.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public abstract class AbstractMetricsTopology<E> implements MetricsTopology {

    protected abstract String getStartedStore();

    protected abstract String getRouteStore();

    protected abstract String getStatusStore();

    protected abstract boolean isStarted(E event);

    protected abstract boolean isRoute(E event);

    protected abstract boolean isStatus(E event);

    protected abstract boolean isFull(E event);

    protected abstract E mergeRoute(E base, E route);

    protected abstract E mergeStatus(E base, E status);

    protected abstract Serde<E> getSerde();

    protected abstract String getKey(E event);

    protected KTable<String, E> toKTable(KStream<String, E> source, String storeName) {
        return source
                .selectKey((k, v) -> getKey(v))
                .groupByKey(Grouped.with(Serdes.String(), getSerde()))
                .reduce((oldV, newV) -> newV, Materialized.as(storeName));
    }

    protected KStream<String, E> buildFull(KStream<String, E> input) {

        KStream<String, E> started = input.filter((k, v) -> isStarted(v));
        KStream<String, E> route = input.filter((k, v) -> isRoute(v));
        KStream<String, E> status = input.filter((k, v) -> isStatus(v));

        KTable<String, E> startedTable = toKTable(started, getStartedStore());
        KTable<String, E> routeTable = toKTable(route, getRouteStore());
        KTable<String, E> statusTable = toKTable(status, getStatusStore());

        return startedTable
                .leftJoin(routeTable, this::mergeRoute)
                .leftJoin(statusTable, this::mergeStatus)
                .toStream()
                .filter((k, v) -> isFull(v));
    }
}
