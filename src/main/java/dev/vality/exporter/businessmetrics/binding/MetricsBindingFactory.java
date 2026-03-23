package dev.vality.exporter.businessmetrics.binding;

import dev.vality.exporter.businessmetrics.model.CustomTag;
import dev.vality.exporter.businessmetrics.model.Metric;
import dev.vality.exporter.businessmetrics.model.payments.PaymentAggregation;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricsStore;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalAggregation;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalMetricsStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class MetricsBindingFactory {

    private final MeterRegistry registry;
    private final PaymentMetricsStore paymentMetricsStore;
    private final WithdrawalMetricsStore withdrawalMetricsStore;

    private static final String PAYMENT_COUNT_SCRAPE = "ebm_payments_count_scrape";
    private static final String PAYMENT_AMOUNT_SCRAPE = "ebm_payments_amount_scrape";
    private static final String WITHDRAWAL_COUNT_SCRAPE = "ebm_withdrawal_count_scrape";
    private static final String WITHDRAWAL_AMOUNT_SCRAPE = "ebm_withdrawal_amount_scrape";

    public MetricsBinding<PaymentMetricsStore.MetricKey, PaymentAggregation> payments() {
        MultiGauge count = MultiGauge.builder(Metric.PAYMENTS_STATUS_COUNT.getName())
                .description(Metric.PAYMENTS_STATUS_COUNT.getDescription())
                .register(registry);

        MultiGauge amount = MultiGauge.builder(Metric.PAYMENTS_AMOUNT.getName())
                .description(Metric.PAYMENTS_AMOUNT.getDescription())
                .register(registry);

        return new MetricsBinding<>(
                count,
                amount,
                paymentMetricsStore.store(),
                this::paymentTags,
                PaymentAggregation::getCount,
                PaymentAggregation::getAmount,
                PaymentMetricsStore.MetricKey::window,
                PaymentAggregation::getLastUpdated,
                paymentMetricsStore::remove,
                PAYMENT_COUNT_SCRAPE,
                PAYMENT_AMOUNT_SCRAPE,
                List.of(
                        Metric.PAYMENTS_STATUS_COUNT.getName(),
                        Metric.PAYMENTS_AMOUNT.getName()
                )
        );
    }

    public MetricsBinding<WithdrawalMetricsStore.MetricKey, WithdrawalAggregation> withdrawals() {
        MultiGauge count = MultiGauge.builder(Metric.WITHDRAWALS_STATUS_COUNT.getName())
                .description(Metric.WITHDRAWALS_STATUS_COUNT.getDescription())
                .register(registry);

        MultiGauge amount = MultiGauge.builder(Metric.WITHDRAWALS_AMOUNT.getName())
                .description(Metric.WITHDRAWALS_AMOUNT.getDescription())
                .register(registry);

        return new MetricsBinding<>(
                count,
                amount,
                withdrawalMetricsStore.store(),
                this::withdrawalTags,
                WithdrawalAggregation::getCount,
                WithdrawalAggregation::getAmount,
                WithdrawalMetricsStore.MetricKey::window,
                WithdrawalAggregation::getLastUpdated,
                withdrawalMetricsStore::remove,
                WITHDRAWAL_COUNT_SCRAPE,
                WITHDRAWAL_AMOUNT_SCRAPE,
                List.of(
                        Metric.WITHDRAWALS_STATUS_COUNT.getName(),
                        Metric.WITHDRAWALS_AMOUNT.getName()
                )
        );
    }

    private Tags paymentTags(PaymentMetricsStore.MetricKey key) {
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

    private Tags withdrawalTags(WithdrawalMetricsStore.MetricKey key) {
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
