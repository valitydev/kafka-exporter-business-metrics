package dev.vality.exporter.businessmetrics.dao;

import dev.vality.exporter.businessmetrics.config.PostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.domain.enums.InvoicePaymentStatus;
import dev.vality.exporter.businessmetrics.domain.enums.WithdrawalStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
import dev.vality.exporter.businessmetrics.domain.tables.records.InvoicePaymentDataRecord;
import dev.vality.exporter.businessmetrics.domain.tables.records.WithdrawalDataRecord;
import dev.vality.exporter.businessmetrics.dto.PaymentStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalStatusMetricRow;
import dev.vality.exporter.businessmetrics.model.MetricWindow;
import dev.vality.exporter.businessmetrics.utils.TestData;
import org.assertj.core.api.AssertionsForClassTypes;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.List;

import static dev.vality.exporter.businessmetrics.domain.Tables.INVOICE_PAYMENT_DATA;
import static dev.vality.exporter.businessmetrics.domain.Tables.WITHDRAWAL_DATA;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@SpringBootTest
public class MetricsDaoImplTest {

    @Autowired
    private MetricsDao metricsDao;

    @Autowired
    private DSLContext dsl;

    @BeforeEach
    void clean() {
        dsl.deleteFrom(INVOICE_PAYMENT_DATA).execute();
        dsl.deleteFrom(WITHDRAWAL_DATA).execute();
    }

    @Test
    void shouldCalculatePaymentMetricsForDifferentWindows() {

        LocalDateTime now = LocalDateTime.now();

        insertPayment(
                "p1",
                now.minusMinutes(2),
                100L,
                InvoicePaymentStatus.captured
        );

        insertPayment(
                "p2",
                now.minusMinutes(10),
                200L,
                InvoicePaymentStatus.captured
        );

        insertPayment(
                "p3",
                now.minusHours(2),
                300L,
                InvoicePaymentStatus.captured
        );

        insertPayment(
                "p4",
                now.minusHours(25),
                999L,
                InvoicePaymentStatus.captured
        );

        List<PaymentStatusMetricRow> rows =
                metricsDao.getPaymentStatusMetrics();

        assertThat(rows).hasSize(1);

        PaymentStatusMetricRow row = rows.getFirst();

        assertThat(row.getMetrics().get(MetricWindow.M5).getCount()).isEqualTo(1L);
        assertThat(row.getMetrics().get(MetricWindow.M5).getAmount()).isEqualTo(100L);

        assertThat(row.getMetrics().get(MetricWindow.M15).getCount()).isEqualTo(2L);
        assertThat(row.getMetrics().get(MetricWindow.M15).getAmount()).isEqualTo(300L);

        assertThat(row.getMetrics().get(MetricWindow.H1).getCount()).isEqualTo(2L);
        assertThat(row.getMetrics().get(MetricWindow.H1).getAmount()).isEqualTo(300L);

        assertThat(row.getMetrics().get(MetricWindow.H24).getCount()).isEqualTo(3L);
        assertThat(row.getMetrics().get(MetricWindow.H24).getAmount()).isEqualTo(600L);
    }

    @Test
    void shouldGroupPaymentsByStatus() {

        LocalDateTime now = LocalDateTime.now();

        insertPayment(
                "p1",
                now.minusMinutes(2),
                100L,
                InvoicePaymentStatus.pending
        );

        insertPayment(
                "p2",
                now.minusMinutes(2),
                200L,
                InvoicePaymentStatus.captured
        );

        List<PaymentStatusMetricRow> rows =
                metricsDao.getPaymentStatusMetrics();

        assertThat(rows).hasSize(2);

        assertThat(rows)
                .extracting(PaymentStatusMetricRow::getPaymentStatus)
                .containsExactlyInAnyOrder(
                        InvoicePaymentStatus.pending,
                        InvoicePaymentStatus.captured
                );
    }

    @Test
    void shouldCalculateWithdrawalMetrics() {

        LocalDateTime now = LocalDateTime.now();

        insertWithdrawal(
                "w1",
                now.minusMinutes(2),
                100L,
                WithdrawalStatus.succeeded
        );

        insertWithdrawal(
                "w2",
                now.minusMinutes(10),
                200L,
                WithdrawalStatus.succeeded
        );

        List<WithdrawalStatusMetricRow> rows =
                metricsDao.getWithdrawalStatusMetrics();

        assertThat(rows).hasSize(1);

        WithdrawalStatusMetricRow row = rows.get(0);

        assertThat(row.getMetrics().get(MetricWindow.M5).getCount()).isEqualTo(1L);
        assertThat(row.getMetrics().get(MetricWindow.M5).getAmount()).isEqualTo(100L);

        assertThat(row.getMetrics().get(MetricWindow.M15).getCount()).isEqualTo(2L);
        assertThat(row.getMetrics().get(MetricWindow.M15).getAmount()).isEqualTo(300L);
    }

    private void insertPayment(
            String paymentId,
            LocalDateTime createdAt,
            long amount,
            InvoicePaymentStatus status
    ) {
        InvoicePaymentData data =
                TestData.paymentData(
                        "invoice-" + paymentId,
                        paymentId,
                        createdAt
                );

        data.setAmount(amount);
        data.setPaymentStatus(status);

        InvoicePaymentDataRecord record =
                dsl.newRecord(INVOICE_PAYMENT_DATA, data);

        dsl.insertInto(INVOICE_PAYMENT_DATA)
                .set(record)
                .execute();
    }

    private void insertWithdrawal(
            String withdrawalId,
            LocalDateTime createdAt,
            long amount,
            WithdrawalStatus status
    ) {
        WithdrawalData data =
                TestData.withdrawalData(
                        withdrawalId,
                        createdAt
                );

        data.setAmount(amount);
        data.setWithdrawalStatus(status);

        WithdrawalDataRecord record =
                dsl.newRecord(WITHDRAWAL_DATA, data);

        dsl.insertInto(WITHDRAWAL_DATA)
                .set(record)
                .execute();
    }

    @Test
    void shouldExcludeRecordsOlderThan24Hours() {

        LocalDateTime now = LocalDateTime.now();

        insertPayment(
                "old",
                now.minusHours(24).minusSeconds(1),
                100L,
                InvoicePaymentStatus.captured
        );

        insertPayment(
                "actual",
                now.minusHours(23).minusMinutes(59),
                200L,
                InvoicePaymentStatus.captured
        );

        List<PaymentStatusMetricRow> rows =
                metricsDao.getPaymentStatusMetrics();

        assertThat(rows).hasSize(1);

        AssertionsForClassTypes.assertThat(rows.getFirst()
                .getMetrics().get(MetricWindow.H24).getCount()).isEqualTo(1L);
        AssertionsForClassTypes.assertThat(rows.getFirst()
                .getMetrics().get(MetricWindow.H24).getAmount()).isEqualTo(200L);
    }
}

