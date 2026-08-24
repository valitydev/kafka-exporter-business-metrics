package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.config.PostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.domain.enums.WithdrawalStatus;
import dev.vality.exporter.businessmetrics.dto.WithdrawalStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalTransactionMetricRow;
import dev.vality.exporter.businessmetrics.factory.MetricGaugeFactory;
import dev.vality.exporter.businessmetrics.factory.MetricTagsFactory;
import dev.vality.exporter.businessmetrics.model.Metric;
import dev.vality.exporter.businessmetrics.model.MetricWindow;
import dev.vality.exporter.businessmetrics.model.TimeWindowMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@PostgresqlSpringBootITest
public class WithdrawalMetricGaugeSpringTest {

    @Autowired
    private WithdrawalMetricGaugeWriter writer;
    @Autowired
    private MetricGaugeFactory factory;
    @Autowired
    private MetricTagsFactory tagsFactory;
    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void shouldWireWithdrawalMetricGaugeWriter() {
        assertThat(writer).isNotNull();
        assertThat(factory).isNotNull();
        assertThat(tagsFactory).isNotNull();
        assertThat(meterRegistry).isNotNull();
    }

    @Test
    void shouldRegisterWithdrawalStatusCountMetrics() {
        WithdrawalStatusMetricRow row = createWithdrawalStatusRow();
        row.getMetrics().get(MetricWindow.M5).setCount(1L);
        row.getMetrics().get(MetricWindow.M15).setCount(2L);
        row.getMetrics().get(MetricWindow.M30).setCount(3L);
        row.getMetrics().get(MetricWindow.H1).setCount(4L);
        row.getMetrics().get(MetricWindow.H3).setCount(5L);
        row.getMetrics().get(MetricWindow.H6).setCount(6L);
        row.getMetrics().get(MetricWindow.H12).setCount(7L);
        row.getMetrics().get(MetricWindow.H24).setCount(8L);
        row.getMetrics().get(MetricWindow.TODAY_MSK).setCount(9L);
        writer.writeStatus(List.of(row));
        var metric = meterRegistry.find(Metric.WITHDRAWALS_STATUS_COUNT.getName());
        assertThat(metric).isNotNull();
        assertThat(metric.gauges().size()).isEqualTo(9);
    }

    @Test
    void shouldRegisterWithdrawalAmountMetrics() {
        WithdrawalStatusMetricRow row = createWithdrawalStatusRow();
        row.getMetrics().get(MetricWindow.M5).setAmount(10L);
        row.getMetrics().get(MetricWindow.M15).setAmount(20L);
        row.getMetrics().get(MetricWindow.M30).setAmount(30L);
        row.getMetrics().get(MetricWindow.H1).setAmount(40L);
        row.getMetrics().get(MetricWindow.H3).setAmount(50L);
        row.getMetrics().get(MetricWindow.H6).setAmount(60L);
        row.getMetrics().get(MetricWindow.H12).setAmount(70L);
        row.getMetrics().get(MetricWindow.H24).setAmount(80L);
        row.getMetrics().get(MetricWindow.TODAY_MSK).setAmount(90L);
        writer.writeStatus(List.of(row));
        var metric = meterRegistry.find(Metric.WITHDRAWALS_AMOUNT.getName());
        assertThat(metric).isNotNull();
        assertThat(metric.gauges().size()).isEqualTo(9);
    }

    @Test
    void shouldRegisterWithdrawalTransactionMetric() {
        WithdrawalTransactionMetricRow row = new WithdrawalTransactionMetricRow();
        row.setProviderId(1);
        row.setProviderName("provider");
        row.setTerminalId(2);
        row.setTerminalName("terminal");
        row.setWalletId("wallet");
        row.setWalletName("wallet name");
        row.setCurrencyCode("RUB");
        row.setCurrencyExponent("2");
        row.setPartyId(UUID.fromString("00000000-0000-0000-0000-000000000001"));
        row.setPartyName("party name");
        row.setCount(42L);
        writer.writeTransactions(List.of(row));
        var metric = meterRegistry.find(Metric.WITHDRAWALS_TRANSACTION_COUNT.getName());
        assertThat(metric).isNotNull();
        assertThat(metric.gauges().size()).isEqualTo(1);
    }

    private WithdrawalStatusMetricRow createWithdrawalStatusRow() {
        WithdrawalStatusMetricRow row = new WithdrawalStatusMetricRow();
        row.setProviderId(1);
        row.setProviderName("provider");
        row.setTerminalId(2);
        row.setTerminalName("terminal");
        row.setWalletId("wallet");
        row.setWalletName("wallet name");
        row.setPartyId(UUID.fromString("00000000-0000-0000-0000-000000000001"));
        row.setPartyName("party name");
        row.setCurrencyCode("RUB");
        row.setWithdrawalStatus(WithdrawalStatus.succeeded);
        row.setCurrencyExponent("2");
        TimeWindowMetrics metrics = new TimeWindowMetrics();

        for (MetricWindow window : MetricWindow.values()) {
            metrics.put(window, 0L, 0L);
        }

        row.setMetrics(metrics);
        return row;
    }
}
