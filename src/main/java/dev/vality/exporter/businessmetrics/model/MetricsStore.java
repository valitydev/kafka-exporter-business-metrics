package dev.vality.exporter.businessmetrics.model;

public interface MetricsStore<M, A> {
    void put(M key, String tag, A aggregation);
}
