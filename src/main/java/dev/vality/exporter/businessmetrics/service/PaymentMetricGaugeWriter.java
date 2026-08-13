package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.dto.PaymentStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.PaymentTransactionMetricRow;
import dev.vality.exporter.businessmetrics.factory.MetricGaugeFactory;
import dev.vality.exporter.businessmetrics.factory.MetricTagsFactory;
import dev.vality.exporter.businessmetrics.model.Metric;
import io.micrometer.core.instrument.MultiGauge;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
@RequiredArgsConstructor
public class PaymentMetricGaugeWriter {

    private final MetricGaugeFactory factory;
    private final MetricTagsFactory tags;

    private MultiGauge countGauge;
    private MultiGauge amountGauge;
    private MultiGauge transactionGauge;

    @PostConstruct
    void init() {
        countGauge = factory.create(Metric.PAYMENTS_STATUS_COUNT);
        amountGauge = factory.create(Metric.PAYMENTS_AMOUNT);
        transactionGauge = factory.create(Metric.PAYMENTS_TRANSACTION_COUNT);
    }

    public void writeStatus(
            List<PaymentStatusMetricRow> rows
    ) {
        countGauge.register(
                rows.stream()
                        .flatMap(row -> toCountRows(row).stream())
                        .toList(),
                true
        );

        amountGauge.register(
                rows.stream()
                        .flatMap(row -> toAmountRows(row).stream())
                        .toList(),
                true
        );
    }

    public void writeTransactions(
            List<PaymentTransactionMetricRow> rows
    ) {
        transactionGauge.register(
                rows.stream()
                        .map(row -> MultiGauge.Row.of(
                                tags.transactionTags(row),
                                row.getCount()
                        ))
                        .toList(),
                true
        );
    }

    private List<MultiGauge.Row<Number>> toCountRows(
            PaymentStatusMetricRow row
    ) {
        return row.getMetrics()
                .getValues()
                .entrySet()
                .stream()
                .map(entry -> MultiGauge.Row.of(
                        tags.paymentTags(
                                row,
                                entry.getKey().getLabel()
                        ),
                        entry.getValue().getCount()
                ))
                .toList();
    }

    private List<MultiGauge.Row<Number>> toAmountRows(
            PaymentStatusMetricRow row
    ) {
        return row.getMetrics()
                .getValues()
                .entrySet()
                .stream()
                .map(entry -> MultiGauge.Row.of(
                        tags.paymentTags(
                                row,
                                entry.getKey().getLabel()
                        ),
                        entry.getValue().getAmount()
                ))
                .toList();
    }
}


