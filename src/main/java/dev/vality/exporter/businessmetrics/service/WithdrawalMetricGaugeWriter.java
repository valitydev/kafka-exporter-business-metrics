package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.dto.WithdrawalStatusMetricRow;
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
public class WithdrawalMetricGaugeWriter {

    private final MetricGaugeFactory factory;
    private final MetricTagsFactory tags;

    private MultiGauge countGauge;
    private MultiGauge amountGauge;

    @PostConstruct
    void init() {
        countGauge = factory.create(Metric.WITHDRAWALS_STATUS_COUNT);
        amountGauge = factory.create(Metric.WITHDRAWALS_AMOUNT);
    }

    public void write(
            List<WithdrawalStatusMetricRow> rows
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

    private List<MultiGauge.Row<Number>> toCountRows(
            WithdrawalStatusMetricRow row
    ) {
        return row.getMetrics()
                .getValues()
                .entrySet()
                .stream()
                .map(entry -> MultiGauge.Row.of(
                        tags.withdrawalTags(
                                row,
                                entry.getKey().getLabel()
                        ),
                        entry.getValue().getCount()
                ))
                .toList();
    }

    private List<MultiGauge.Row<Number>> toAmountRows(
            WithdrawalStatusMetricRow row
    ) {
        return row.getMetrics()
                .getValues()
                .entrySet()
                .stream()
                .map(entry -> MultiGauge.Row.of(
                        tags.withdrawalTags(
                                row,
                                entry.getKey().getLabel()
                        ),
                        entry.getValue().getAmount()
                ))
                .toList();
    }
}
