package dev.vality.exporter.businessmetrics.handler.invoice;

import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.dao.DaoException;
import dev.vality.exporter.businessmetrics.dao.InvoicePaymentDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.exception.StorageException;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InvoicePaymentRouteChangedEventHandler implements InvoiceEventHandler {

    private final InvoicePaymentDao invoicePaymentDao;

    @Override
    public boolean accept(InvoicePaymentChange change) {
        return change.getPayload().isSetInvoicePaymentRouteChanged();
    }

    @Override
    public void handle(InvoicePaymentChange change, MachineEvent event) {
        try {
            log.info("Trying to handle InvoicePaymentRouteChanged: eventId={}, invoiceId={}", event.getEventId(),
                    event.getSourceId());
            InvoicePaymentData invoicePaymentData = invoicePaymentDao.get(event.getSourceId(), change.getId());
            if (invoicePaymentData == null) {
                log.warn("InvoicePayment with invoiceId={} not found, skipped", event.getSourceId());
                return;
            }
            var payload = change.getPayload();
            var invoicePaymentStarted = payload.getInvoicePaymentRouteChanged();
            invoicePaymentData.setInvoiceId(event.getSourceId());
            invoicePaymentData.setPaymentId(change.getId());
            invoicePaymentData.setProviderId(invoicePaymentStarted.getRoute().getProvider().getId());
            invoicePaymentData.setTerminalId(invoicePaymentStarted.getRoute().getTerminal().getId());
            Long id = invoicePaymentDao.save(invoicePaymentData);
            log.info("InvoicePaymentRouteChanged has {} been saved: eventId={}, invoiceId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }
}
