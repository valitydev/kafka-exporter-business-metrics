package dev.vality.exporter.businessmetrics.dao;

import dev.vality.dao.GenericDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;

public interface InvoicePaymentDao extends GenericDao {

    InvoicePaymentData get(String invoiceId, String paymentId);

    Long save(InvoicePaymentData invoicePayment);

}
