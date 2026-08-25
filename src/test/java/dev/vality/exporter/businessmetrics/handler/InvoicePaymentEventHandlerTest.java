package dev.vality.exporter.businessmetrics.handler;

import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.damsel.payment_processing.InvoicingSrv;
import dev.vality.dao.DaoException;
import dev.vality.exporter.businessmetrics.config.PostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.dao.InvoicePaymentDao;
import dev.vality.exporter.businessmetrics.domain.enums.InvoicePaymentStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.exception.NotFoundException;
import dev.vality.exporter.businessmetrics.exception.StorageException;
import dev.vality.exporter.businessmetrics.handler.invoice.InvoicePaymentRouteChangedEventHandler;
import dev.vality.exporter.businessmetrics.handler.invoice.InvoicePaymentStartedEventHandler;
import dev.vality.exporter.businessmetrics.handler.invoice.InvoicePaymentStatusChangedEventHandler;
import dev.vality.exporter.businessmetrics.utils.TestData;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.sink.common.serialization.impl.PaymentEventPayloadDeserializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.LocalDateTime;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@PostgresqlSpringBootITest
class InvoicePaymentEventHandlerTest {

    @MockitoBean
    private InvoicePaymentDao invoicePaymentDao;

    @MockitoBean
    private InvoicingSrv.Iface invoicingClient;

    @Autowired
    private InvoicePaymentStartedEventHandler invoicePaymentStartedEventHandler;

    @Autowired
    private InvoicePaymentRouteChangedEventHandler invoicePaymentRouteChangedEventHandler;

    @Autowired
    private InvoicePaymentStatusChangedEventHandler invoicePaymentStatusChangedEventHandler;

    @Test
    void shouldSavePaymentData() throws Exception {
        String invoiceId = "invoice-1";

        MachineEvent event = TestData.getStartedInvoicePaymentEvents(invoiceId);

        InvoicePaymentChange change = extractPaymentChange(event);

        when(invoicePaymentDao.save(any(InvoicePaymentData.class)))
                .thenReturn(1L);

        invoicePaymentStartedEventHandler.handle(change, event);

        ArgumentCaptor<InvoicePaymentData> captor =
                ArgumentCaptor.forClass(InvoicePaymentData.class);

        verify(invoicePaymentDao).save(captor.capture());

        InvoicePaymentData actual = captor.getValue();

        assertThat(actual.getInvoiceId()).isEqualTo(invoiceId);
        assertThat(actual.getPaymentId()).isEqualTo("1");
        assertThat(actual.getShopId()).isEqualTo(TestData.TEST_SHOP_ID);
        assertThat(actual.getCurrencyCode()).isEqualTo("RUB");
        assertThat(actual.getAmount()).isEqualTo(11L);
        assertThat(actual.getPaymentStatus())
                .isEqualTo(InvoicePaymentStatus.pending);
    }

    @Test
    void shouldThrowStorageExceptionWhenDaoFails() throws Exception {
        MachineEvent event = TestData.getStartedInvoicePaymentEvents("invoice-1");
        InvoicePaymentChange change = extractPaymentChange(event);

        when(invoicePaymentDao.save(any()))
                .thenThrow(new DaoException("DB error"));

        assertThatThrownBy(() -> invoicePaymentStartedEventHandler.handle(change, event))
                .isInstanceOf(StorageException.class);

        verify(invoicePaymentDao).save(any());
    }

    @Test
    void shouldUpdateProviderAndTerminal() throws Exception {
        String invoiceId = "invoice-1";

        InvoicePaymentData existing =
                TestData.paymentData(
                        invoiceId,
                        "1",
                        LocalDateTime.now()
                );

        when(invoicePaymentDao.get(invoiceId, "1"))
                .thenReturn(existing);

        when(invoicePaymentDao.save(any()))
                .thenReturn(1L);

        MachineEvent event =
                TestData.getRouteChangedInvoicePaymentEvents(invoiceId);

        InvoicePaymentChange change = extractPaymentChange(event);

        invoicePaymentRouteChangedEventHandler.handle(change, event);

        ArgumentCaptor<InvoicePaymentData> captor =
                ArgumentCaptor.forClass(InvoicePaymentData.class);

        verify(invoicePaymentDao).save(captor.capture());

        InvoicePaymentData actual = captor.getValue();

        assertThat(actual.getProviderId()).isEqualTo(21);
        assertThat(actual.getTerminalId()).isEqualTo(35);
    }

    @Test
    void shouldThrowNotFoundWhenPaymentDoesNotExist() throws Exception {
        String invoiceId = "invoice-1";

        when(invoicePaymentDao.get(invoiceId, "1"))
                .thenReturn(null);

        when(invoicingClient.get(any(), any()))
                .thenReturn(null);

        MachineEvent event =
                TestData.getRouteChangedInvoicePaymentEvents(invoiceId);

        InvoicePaymentChange change = extractPaymentChange(event);

        assertThatThrownBy(() -> invoicePaymentRouteChangedEventHandler.handle(change, event))
                .isInstanceOf(NotFoundException.class);

        verify(invoicePaymentDao, never()).save(any());
    }

    @Test
    void shouldUpdatePaymentStatus() throws Exception {
        String invoiceId = "invoice-1";

        InvoicePaymentData existing =
                TestData.paymentData(
                        invoiceId,
                        "1",
                        LocalDateTime.now()
                );

        existing.setPaymentStatus(InvoicePaymentStatus.pending);

        when(invoicePaymentDao.get(invoiceId, "1"))
                .thenReturn(existing);

        when(invoicePaymentDao.save(any(InvoicePaymentData.class)))
                .thenReturn(1L);

        MachineEvent event =
                TestData.getStatusChangedPaymentEvents(invoiceId);

        InvoicePaymentChange change = extractPaymentChange(event);

        invoicePaymentStatusChangedEventHandler.handle(change, event);

        ArgumentCaptor<InvoicePaymentData> captor =
                ArgumentCaptor.forClass(InvoicePaymentData.class);

        verify(invoicePaymentDao).save(captor.capture());

        InvoicePaymentData actual = captor.getValue();

        assertThat(actual.getInvoiceId())
                .isEqualTo(invoiceId);

        assertThat(actual.getPaymentId())
                .isEqualTo("1");

        assertThat(actual.getPaymentStatus())
                .isEqualTo(InvoicePaymentStatus.captured);
    }

    @Test
    void shouldThrowNotFoundWhenPaymentDoesNotExistForStatusChange()
            throws Exception {

        String invoiceId = "invoice-1";

        when(invoicePaymentDao.get(invoiceId, "1"))
                .thenReturn(null);

        when(invoicingClient.get(any(), any()))
                .thenReturn(null);

        MachineEvent event =
                TestData.getStatusChangedPaymentEvents(invoiceId);

        InvoicePaymentChange change = extractPaymentChange(event);

        assertThatThrownBy(() ->
                invoicePaymentStatusChangedEventHandler.handle(change, event)
        )
                .isInstanceOf(NotFoundException.class);

        verify(invoicePaymentDao)
                .get(invoiceId, "1");

        verify(invoicePaymentDao, never())
                .save(any());
    }

    @Test
    void shouldThrowStorageExceptionWhenDaoFailsOnGet()
            throws Exception {

        String invoiceId = "invoice-1";

        when(invoicePaymentDao.get(invoiceId, "1"))
                .thenThrow(new DaoException("DB error"));

        MachineEvent event =
                TestData.getStatusChangedPaymentEvents(invoiceId);

        InvoicePaymentChange change = extractPaymentChange(event);

        assertThatThrownBy(() ->
                invoicePaymentStatusChangedEventHandler.handle(change, event)
        )
                .isInstanceOf(StorageException.class);

        verify(invoicePaymentDao)
                .get(invoiceId, "1");

        verify(invoicePaymentDao, never())
                .save(any());
    }

    private InvoicePaymentChange extractPaymentChange(MachineEvent event) {
        PaymentEventPayloadDeserializer payloadDeserializer = new PaymentEventPayloadDeserializer();
        EventPayload payload = payloadDeserializer.deserialize(event.data.getBin());
        return payload.getInvoiceChanges()
                .stream()
                .filter(InvoiceChange::isSetInvoicePaymentChange)
                .map(InvoiceChange::getInvoicePaymentChange)
                .findFirst()
                .orElseThrow();
    }
}
