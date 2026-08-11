package dev.vality.exporter.businessmetrics.dao;

import dev.vality.exporter.businessmetrics.dto.PaymentStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.PaymentTransactionMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalStatusMetricRow;

import java.time.LocalDateTime;
import java.util.List;

public interface MetricsDao {

    List<PaymentStatusMetricRow> getPaymentStatusMetrics();

    List<PaymentTransactionMetricRow> getPaymentTransactionMetrics(LocalDateTime from);

    List<WithdrawalStatusMetricRow> getWithdrawalStatusMetrics();
}
