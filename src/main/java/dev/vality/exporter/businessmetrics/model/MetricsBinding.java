package dev.vality.exporter.businessmetrics.model;

import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

public record MetricsBinding<K, A>(
        MultiGauge gaugeCount,
        MultiGauge gaugeAmount,
        Map<K, A> store,
        Function<K, Tags> tagsExtractor,
        ToDoubleFunction<A> countExtractor,
        ToDoubleFunction<A> amountExtractor,
        Function<K, String> windowExtractor,
        Function<A, Instant> lastUpdatedExtractor,
        Consumer<K> remover,
        String countScrapeName,
        String amountScrapeName,
        List<String> metricNames
) {}
