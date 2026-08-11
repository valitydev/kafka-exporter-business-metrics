package dev.vality.exporter.businessmetrics.handler.invoice;

import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.dao.DaoException;
import dev.vality.exporter.businessmetrics.dao.InvoicePaymentDao;
import dev.vality.exporter.businessmetrics.domain.enums.InvoicePaymentStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.exception.StorageException;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InvoicePaymentStartedEventHandler implements InvoiceEventHandler {

    private final InvoicePaymentDao invoicePaymentDao;

    @Override
    public boolean accept(InvoicePaymentChange change) {
        return change.getPayload().isSetInvoicePaymentStarted();
    }

    @Override
    public void handle(InvoicePaymentChange change, MachineEvent event) {
        try {
            log.info("Trying to handle InvoicePaymentStartedCreated: eventId={}, invoiceId={}", event.getEventId(),
                    event.getSourceId());
            var payload = change.getPayload();
            var invoicePaymentStarted = payload.getInvoicePaymentStarted();
            var payment = invoicePaymentStarted.getPayment();
            InvoicePaymentData invoicePaymentData = new InvoicePaymentData();
            invoicePaymentData.setInvoiceId(event.getSourceId());
            invoicePaymentData.setPaymentId(payment.getId());
            if (invoicePaymentStarted.isSetRoute()) {
                invoicePaymentData.setProviderId(invoicePaymentStarted.getRoute().getProvider().getId());
                invoicePaymentData.setTerminalId(invoicePaymentStarted.getRoute().getTerminal().getId());
            }
            invoicePaymentData.setPartyId(UUID.fromString(payment.getPartyRef().getId()));
            invoicePaymentData.setShopId(payment.getShopRef().getId());
            invoicePaymentData.setCurrencyCode(payment.getCost().getCurrency().getSymbolicCode());
            invoicePaymentData.setAmount(payment.getCost().getAmount());
            invoicePaymentData.setPaymentStatus(TBaseUtil.unionFieldToEnum(payment.getStatus(),
                    InvoicePaymentStatus.class));
            LocalDateTime createdAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
            invoicePaymentData.setCreatedAt(createdAt);
            Long id = invoicePaymentDao.save(invoicePaymentData);
            log.info("InvoicePaymentStartedCreated has {} been saved: eventId={}, invoiceId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }
}
