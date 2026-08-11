package dev.vality.exporter.businessmetrics.dao;

import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
import dev.vality.exporter.businessmetrics.domain.tables.records.InvoicePaymentDataRecord;
import dev.vality.exporter.businessmetrics.domain.tables.records.WithdrawalDataRecord;
import dev.vality.exporter.businessmetrics.service.DataCleanupService;
import dev.vality.exporter.businessmetrics.utils.TestData;
import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;

import static dev.vality.exporter.businessmetrics.domain.Tables.INVOICE_PAYMENT_DATA;
import static dev.vality.exporter.businessmetrics.domain.Tables.WITHDRAWAL_DATA;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@PostgresqlTestcontainer
@SpringBootTest
class DataCleanupServiceTest {

    @Autowired
    private DataCleanupService cleanupService;

    @Autowired
    private DSLContext dsl;

    @BeforeEach
    void clean() {
        dsl.deleteFrom(INVOICE_PAYMENT_DATA).execute();
        dsl.deleteFrom(WITHDRAWAL_DATA).execute();
    }

    @Test
    void shouldDeletePaymentsOlderThanFiveDays() {
        LocalDateTime now = LocalDateTime.now();

        insertPayment(
                "old",
                now.minusDays(6)
        );

        insertPayment(
                "actual",
                now.minusDays(2)
        );

        cleanupService.cleanup();

        assertThat(
                dsl.selectCount()
                        .from(INVOICE_PAYMENT_DATA)
                        .fetchOne(0, Integer.class)
        ).isEqualTo(1);

        assertThat(
                dsl.selectFrom(INVOICE_PAYMENT_DATA)
                        .fetchOne()
                        .getPaymentId()
        ).isEqualTo("actual");
    }

    @Test
    void shouldDeleteOldWithdrawals() {

        LocalDateTime now = LocalDateTime.now();

        insertWithdrawal(
                "old",
                now.minusDays(6)
        );

        insertWithdrawal(
                "actual",
                now.minusDays(2)
        );

        cleanupService.cleanup();

        assertThat(
                dsl.selectCount()
                        .from(WITHDRAWAL_DATA)
                        .fetchOne(0, Integer.class)
        ).isEqualTo(1);
    }

    private void insertPayment(
            String paymentId,
            LocalDateTime createdAt
    ) {
        InvoicePaymentData data =
                TestData.paymentData(
                        "invoice-" + paymentId,
                        paymentId,
                        createdAt
                );

        InvoicePaymentDataRecord record =
                dsl.newRecord(INVOICE_PAYMENT_DATA, data);

        dsl.insertInto(INVOICE_PAYMENT_DATA)
                .set(record)
                .execute();
    }

    private void insertWithdrawal(
            String id,
            LocalDateTime createdAt
    ) {
        WithdrawalData data =
                TestData.withdrawalData(id, createdAt);

        WithdrawalDataRecord record =
                dsl.newRecord(WITHDRAWAL_DATA, data);

        dsl.insertInto(WITHDRAWAL_DATA)
                .set(record)
                .execute();
    }
}
