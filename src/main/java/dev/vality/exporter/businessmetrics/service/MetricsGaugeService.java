package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.dao.MetricsDao;
import dev.vality.exporter.businessmetrics.resolver.MetricLabelResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MetricsGaugeService {

    private final MetricsDao metricsDao;

    private final MetricLabelResolver resolver;

    private final PaymentMetricGaugeWriter paymentWriter;

    private final WithdrawalMetricGaugeWriter withdrawalWriter;


    @Scheduled(fixedRateString = "${exporter.metrics.refresh-delay-ms}")
    public void refresh() {
        try {
            refreshPayments();
            refreshTransactionsPayments();
            refreshWithdrawals();
            refreshTransactionsWithdrawals();
        } catch (Exception e) {
            log.error(
                    "Cannot refresh metrics",
                    e
            );
        }
    }

    private void refreshPayments() {
        var rows =
                metricsDao.getPaymentStatusMetrics()
                        .stream()
                        .map(resolver::resolve)
                        .toList();
        paymentWriter.writeStatus(rows);
    }

    private void refreshTransactionsPayments() {
        var rows =
                metricsDao.getPaymentTransactionMetrics()
                        .stream()
                        .map(resolver::resolve)
                        .toList();
        paymentWriter.writeTransactions(rows);
    }

    private void refreshWithdrawals() {
        var rows =
                metricsDao.getWithdrawalStatusMetrics()
                        .stream()
                        .map(resolver::resolve)
                        .toList();
        withdrawalWriter.writeStatus(rows);
    }

    private void refreshTransactionsWithdrawals() {
        var rows =
                metricsDao.getWithdrawalTransactionMetrics()
                        .stream()
                        .map(resolver::resolve)
                        .toList();
        withdrawalWriter.writeTransactions(rows);
    }
}