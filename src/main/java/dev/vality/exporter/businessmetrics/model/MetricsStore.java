package dev.vality.exporter.businessmetrics.model;

import java.time.LocalDate;

public interface MetricsStore<M, A> {
    void put(M key, String tag, LocalDate date, A aggregation);
}
