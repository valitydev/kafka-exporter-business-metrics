package dev.vality.exporter.businessmetrics.handler.invoice;

import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.dao.DaoException;
import dev.vality.exporter.businessmetrics.dao.InvoicePaymentDao;
import dev.vality.exporter.businessmetrics.domain.enums.InvoicePaymentStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.exception.NotFoundException;
import dev.vality.exporter.businessmetrics.exception.StorageException;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InvoicePaymentStatusChangedEventHandler implements InvoiceEventHandler {

    private final InvoicePaymentDao invoicePaymentDao;

    @Override
    public boolean accept(InvoicePaymentChange change) {
        return change.getPayload().isSetInvoicePaymentStatusChanged();
    }

    @Override
    public void handle(InvoicePaymentChange change, MachineEvent event) {
        try {
            log.info("Trying to handle InvoicePaymentStatusChanged: eventId={}, invoiceId={}", event.getEventId(),
                    event.getSourceId());
            var payload = change.getPayload();
            var invoicePaymentStatusChanged = payload.getInvoicePaymentStatusChanged();
            InvoicePaymentData invoicePaymentData = getInvoicePaymentData(event.getSourceId(), change.getId());
            invoicePaymentData.setInvoiceId(event.getSourceId());
            invoicePaymentData.setPaymentId(change.getId());
            invoicePaymentData.setPaymentStatus(
                    TBaseUtil.unionFieldToEnum(invoicePaymentStatusChanged.getStatus(),
                            InvoicePaymentStatus.class));
            Long id = invoicePaymentDao.save(invoicePaymentData);
            log.info("InvoicePaymentStatusChanged has {} been saved: eventId={}, withdrawalId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    private InvoicePaymentData getInvoicePaymentData(String invoiceId, String paymentId) throws DaoException {
        InvoicePaymentData invoicePaymentData = invoicePaymentDao.get(invoiceId, paymentId);

        if (invoicePaymentData == null) {
            throw new NotFoundException(
                    String.format("InvoicePayment with invoiceId='%s' not found", invoiceId));
        }

        return invoicePaymentData;
    }
}
