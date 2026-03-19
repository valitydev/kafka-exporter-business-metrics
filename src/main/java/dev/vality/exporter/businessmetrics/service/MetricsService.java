package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.model.CustomTag;
import dev.vality.exporter.businessmetrics.model.Metric;
import dev.vality.exporter.businessmetrics.model.MetricsWindows;
import dev.vality.exporter.businessmetrics.model.payments.PaymentAggregation;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricsStore;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalAggregation;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalMetricsStore;
import io.micrometer.core.instrument.*;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsService {
    private final PaymentMetricsStore paymentMetricsStore;
    private final WithdrawalMetricsStore withdrawalMetricsStore;
    private final MeterRegistry registry;

    private MultiGauge paymentsCount;
    private MultiGauge paymentsAmount;
    private MultiGauge withdrawalsCount;
    private MultiGauge withdrawalsAmount;

    private static final String PAYMENT_COUNT_SCRAPE = "ebm_payments_count_scrape";
    private static final String PAYMENT_AMOUNT_SCRAPE = "ebm_payments_amount_scrape";
    private static final String WITHDRAWAL_COUNT_SCRAPE = "ebm_withdrawal_count_scrape";
    private static final String WITHDRAWAL_AMOUNT_SCRAPE = "ebm_withdrawal_amount_scrape";

    @PostConstruct
    void init() {
        paymentsCount = MultiGauge.builder(Metric.PAYMENTS_STATUS_COUNT.getName())
                .description(Metric.PAYMENTS_STATUS_COUNT.getDescription())
                .register(registry);

        paymentsAmount = MultiGauge.builder(Metric.PAYMENTS_AMOUNT.getName())
                .description(Metric.PAYMENTS_AMOUNT.getDescription())
                .register(registry);

        registry.gauge(PAYMENT_COUNT_SCRAPE, paymentMetricsStore, store -> {
            updateMultiGauge(
                    paymentsCount,
                    paymentMetricsStore.store(),
                    this::getPaymentTags,
                    PaymentAggregation::getCount
            );
            return paymentMetricsStore.store().size();
        });

        registry.gauge(PAYMENT_AMOUNT_SCRAPE, paymentMetricsStore, store -> {
            updateMultiGauge(
                    paymentsAmount,
                    paymentMetricsStore.store(),
                    this::getPaymentTags,
                    PaymentAggregation::getAmount
            );
            return paymentMetricsStore.store().size();
        });

        withdrawalsCount = MultiGauge.builder(Metric.WITHDRAWALS_STATUS_COUNT.getName())
                .description(Metric.WITHDRAWALS_STATUS_COUNT.getDescription())
                .register(registry);

        withdrawalsAmount = MultiGauge.builder(Metric.WITHDRAWALS_AMOUNT.getName())
                .description(Metric.WITHDRAWALS_AMOUNT.getDescription())
                .register(registry);

        registry.gauge(WITHDRAWAL_COUNT_SCRAPE, withdrawalMetricsStore, store -> {
            updateMultiGauge(
                    withdrawalsCount,
                    withdrawalMetricsStore.store(),
                    this::getWithdrawalTags,
                    WithdrawalAggregation::getCount
            );
            return withdrawalMetricsStore.store().size();
        });

        registry.gauge(WITHDRAWAL_AMOUNT_SCRAPE, withdrawalMetricsStore, store -> {
            updateMultiGauge(
                    withdrawalsAmount,
                    withdrawalMetricsStore.store(),
                    this::getWithdrawalTags,
                    WithdrawalAggregation::getAmount
            );
            return withdrawalMetricsStore.store().size();
        });
    }

    private <K, A> void updateMultiGauge(
            MultiGauge gauge,
            Map<K, A> store,
            Function<K, Tags> tagsExtractor,
            ToDoubleFunction<A> valueExtractor
    ) {
        List<MultiGauge.Row<Number>> rows = store.entrySet().stream()
                .map(entry ->
                        MultiGauge.Row.of(
                                tagsExtractor.apply(entry.getKey()),
                                valueExtractor.applyAsDouble(entry.getValue())
                        ))
                .toList();

        gauge.register(rows, true);
    }

    @Value("${metrics.ttl.seconds}")
    private long defaultTtlSeconds;

    @Value("${metrics.ttl.min-lifetime-seconds}")
    private long minLifetimeSeconds;

    @Value("${metrics.ttl.cleaner.enabled}")
    private boolean enabledClean;

    @Scheduled(fixedDelayString = "${metrics.ttl.cleaner.ms}")
    private void cleanOldMetrics() {
        if (!enabledClean) {
            return;
        }
        log.debug("Start clean old metrics");
        cleanStore(
                paymentMetricsStore.store(),
                this::getPaymentTags,
                PaymentMetricsStore.MetricKey::window,
                PaymentAggregation::getLastUpdated,
                paymentMetricsStore::remove,
                List.of(
                        Metric.PAYMENTS_STATUS_COUNT.getName(),
                        Metric.PAYMENTS_AMOUNT.getName()
                )
        );

        cleanStore(
                withdrawalMetricsStore.store(),
                this::getWithdrawalTags,
                WithdrawalMetricsStore.MetricKey::window,
                WithdrawalAggregation::getLastUpdated,
                withdrawalMetricsStore::remove,
                List.of(
                        Metric.WITHDRAWALS_STATUS_COUNT.getName(),
                        Metric.WITHDRAWALS_AMOUNT.getName()
                )
        );

    }

    private <K, A> void cleanStore(
            Map<K, A> store,
            Function<K, Tags> tagsExtractor,
            Function<K, String> windowExtractor,
            Function<A, Instant> lastUpdatedExtractor,
            Consumer<K> remover,
            List<String> metricNames
    ) {
        Instant now = Instant.now();

        store.forEach((key, aggregation) -> {
            Instant lastUpdated = lastUpdatedExtractor.apply(aggregation);

            if (lastUpdated.plusSeconds(minLifetimeSeconds).isAfter(now)) {
                return;
            }

            long ttl = MetricsWindows.WINDOW_TTL_SECONDS
                    .getOrDefault(windowExtractor.apply(key), defaultTtlSeconds);

            boolean expired = lastUpdated.plusSeconds(ttl).isBefore(now);

            if (expired) {
                Tags tags = tagsExtractor.apply(key);

                for (String metricName : metricNames) {
                    Meter meter = registry.find(metricName)
                            .tags(tags)
                            .meter();
                    if (meter != null) {
                        registry.remove(meter);
                    }
                }

                remover.accept(key);

                log.info("Removed expired metric for key={} with TTL={}s", key, ttl);
            }
        });
    }

    private Tags getPaymentTags(PaymentMetricsStore.MetricKey key) {
        return Tags.of(
                CustomTag.providerId(String.valueOf(key.providerId())),
                CustomTag.terminalId(String.valueOf(key.terminalId())),
                CustomTag.shopId(key.shopId()),
                CustomTag.currency(key.currency()),
                CustomTag.status(key.status()),
                CustomTag.duration(key.window()),
                Tag.of("date", key.date().toString())
        );
    }

    private Tags getWithdrawalTags(WithdrawalMetricsStore.MetricKey key) {
        return Tags.of(
                CustomTag.providerId(String.valueOf(key.providerId())),
                CustomTag.terminalId(String.valueOf(key.terminalId())),
                CustomTag.shopId(key.walletId()),
                CustomTag.currency(key.currency()),
                CustomTag.status(key.status()),
                CustomTag.duration(key.window()),
                Tag.of("date", key.date().toString())
        );
    }
}
