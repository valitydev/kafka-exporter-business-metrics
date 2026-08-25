package dev.vality.exporter.businessmetrics.service;

import dev.vality.damsel.payment_processing.EventRange;
import dev.vality.damsel.payment_processing.Invoice;
import dev.vality.damsel.payment_processing.InvoicePayment;
import dev.vality.damsel.payment_processing.InvoicingSrv;
import dev.vality.dao.DaoException;
import dev.vality.exporter.businessmetrics.dao.InvoicePaymentDao;
import dev.vality.exporter.businessmetrics.domain.enums.InvoicePaymentStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.exception.NotFoundException;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class InvoicePaymentClient {

    private final InvoicePaymentDao invoicePaymentDao;
    private final InvoicingSrv.Iface invoicingClient;

    public InvoicePaymentData getInvoicePaymentData(
            String invoiceId,
            String paymentId,
            Long eventId) throws DaoException {
        InvoicePaymentData invoicePaymentData = invoicePaymentDao.get(invoiceId, paymentId);
        if (invoicePaymentData != null) {
            return invoicePaymentData;
        }
        return loadInvoiceFromProcessing(invoiceId, paymentId, eventId);
    }

    @SneakyThrows
    private InvoicePaymentData loadInvoiceFromProcessing(String invoiceId, String paymentId, Long eventId) {
        Invoice invoice = invoicingClient.get(invoiceId, new EventRange().setLimit(Math.toIntExact(eventId)));
        if (invoice == null) {
            throw new NotFoundException(
                    String.format("InvoicePaymentEvent with invoiceId='%s' not found", invoiceId));
        }
        InvoicePayment payment = invoice.getPayments().stream()
                .filter(it -> it.isSetPayment() && it.getPayment().getId().equals(paymentId))
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                        String.format("InvoicePayment='%s' with invoiceId='%s' not found!", paymentId, invoiceId)));
        return convertStateToInvoicePaymentData(invoice, payment);
    }

    private InvoicePaymentData convertStateToInvoicePaymentData(Invoice invoice, InvoicePayment invoicePayment) {
        InvoicePaymentData invoicePaymentData = new InvoicePaymentData();
        invoicePaymentData.setInvoiceId(invoice.getInvoice().getId());
        invoicePaymentData.setPaymentId(invoicePayment.getPayment().getId());
        if (invoicePayment.isSetRoute()) {
            invoicePaymentData.setProviderId(invoicePayment.getRoute().getProvider().getId());
            invoicePaymentData.setTerminalId(invoicePayment.getRoute().getTerminal().getId());
        }
        invoicePaymentData.setPartyId(UUID.fromString(invoice.getInvoice().getPartyRef().getId()));
        invoicePaymentData.setShopId(invoice.getInvoice().getShopRef().getId());
        invoicePaymentData.setCurrencyCode(invoicePayment.getPayment().getCost().getCurrency().getSymbolicCode());
        invoicePaymentData.setAmount(invoicePayment.getPayment().getCost().getAmount());
        invoicePaymentData.setPaymentStatus(TBaseUtil.unionFieldToEnum(invoicePayment.getPayment().getStatus(),
                InvoicePaymentStatus.class));
        LocalDateTime createdAt = TypeUtil.stringToLocalDateTime(invoicePayment.getPayment().getCreatedAt());
        invoicePaymentData.setCreatedAt(createdAt);
        return invoicePaymentData;
    }
}
