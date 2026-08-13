package dev.vality.exporter.businessmetrics.dao;

import dev.vality.exporter.businessmetrics.config.PostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.domain.enums.InvoicePaymentStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.utils.TestData;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;

import static dev.vality.exporter.businessmetrics.domain.Tables.INVOICE_PAYMENT_DATA;
import static org.assertj.core.api.Assertions.assertThat;

@PostgresqlSpringBootITest
class InvoicePaymentDaoImplTest {

    @Autowired
    private InvoicePaymentDao invoicePaymentDao;

    @Autowired
    private DSLContext dsl;

    @BeforeEach
    void clean() {
        dsl.deleteFrom(INVOICE_PAYMENT_DATA).execute();
    }

    @Test
    void shouldSaveAndGetPayment() throws Exception {
        LocalDateTime now = LocalDateTime.now();

        InvoicePaymentData expected =
                TestData.paymentData(
                        "invoice-1",
                        "payment-1",
                        now
                );

        Long id = invoicePaymentDao.save(expected);

        assertThat(id).isNotNull();

        InvoicePaymentData actual =
                invoicePaymentDao.get("invoice-1", "payment-1");

        assertThat(actual).isNotNull();
        assertThat(actual.getInvoiceId()).isEqualTo("invoice-1");
        assertThat(actual.getPaymentId()).isEqualTo("payment-1");
        assertThat(actual.getAmount()).isEqualTo(100L);
        assertThat(actual.getCurrencyCode()).isEqualTo("RUB");
        assertThat(actual.getProviderId()).isEqualTo(21);
        assertThat(actual.getTerminalId()).isEqualTo(35);
    }

    @Test
    void shouldUpdateExistingPaymentOnConflict() throws Exception {
        InvoicePaymentData payment =
                TestData.paymentData(
                        "invoice-1",
                        "payment-1",
                        LocalDateTime.now()
                );

        invoicePaymentDao.save(payment);

        payment.setAmount(999L);
        payment.setPaymentStatus(InvoicePaymentStatus.captured);

        invoicePaymentDao.save(payment);

        InvoicePaymentData actual =
                invoicePaymentDao.get("invoice-1", "payment-1");

        assertThat(actual.getAmount()).isEqualTo(999L);
        assertThat(actual.getPaymentStatus())
                .isEqualTo(InvoicePaymentStatus.captured);
    }

    @Test
    void shouldReturnNullWhenPaymentDoesNotExist() throws Exception {
        InvoicePaymentData result =
                invoicePaymentDao.get("unknown", "unknown");

        assertThat(result).isNull();
    }
}
