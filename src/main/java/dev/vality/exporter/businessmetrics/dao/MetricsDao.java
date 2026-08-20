package dev.vality.exporter.businessmetrics.dao;

import dev.vality.exporter.businessmetrics.dto.PaymentStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.PaymentTransactionMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalTransactionMetricRow;

import java.util.List;

public interface MetricsDao {

    List<PaymentStatusMetricRow> getPaymentStatusMetrics();

    List<PaymentTransactionMetricRow> getPaymentTransactionMetrics();

    List<WithdrawalStatusMetricRow> getWithdrawalStatusMetrics();

    List<WithdrawalTransactionMetricRow> getWithdrawalTransactionMetrics();
}
