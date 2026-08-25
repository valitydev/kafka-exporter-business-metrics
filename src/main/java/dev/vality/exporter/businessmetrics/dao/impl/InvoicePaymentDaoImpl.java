package dev.vality.exporter.businessmetrics.dao.impl;

import dev.vality.dao.DaoException;
import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.exporter.businessmetrics.dao.InvoicePaymentDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.domain.tables.records.InvoicePaymentDataRecord;
import dev.vality.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static dev.vality.exporter.businessmetrics.domain.Tables.INVOICE_PAYMENT_DATA;

@Component
public class InvoicePaymentDaoImpl extends AbstractGenericDao implements InvoicePaymentDao {

    private final RecordRowMapper<InvoicePaymentData> invoicePaymentRecordRowMapper;

    public InvoicePaymentDaoImpl(DataSource dataSource) {
        super(dataSource);
        invoicePaymentRecordRowMapper = new RecordRowMapper<>(INVOICE_PAYMENT_DATA, InvoicePaymentData.class);
    }

    @Override
    public InvoicePaymentData get(String invoiceId, String paymentId) throws DaoException {
        Query query = getDslContext()
                .selectFrom(INVOICE_PAYMENT_DATA)
                .where(INVOICE_PAYMENT_DATA.INVOICE_ID.eq(invoiceId))
                .and(INVOICE_PAYMENT_DATA.PAYMENT_ID.eq(paymentId));

        return fetchOne(query, invoicePaymentRecordRowMapper);
    }

    @Override
    public Long save(InvoicePaymentData invoicePaymentData) throws DaoException {
        InvoicePaymentDataRecord invoicePaymentRecord = getDslContext().newRecord(INVOICE_PAYMENT_DATA, invoicePaymentData);

        Query query = getDslContext().insertInto(INVOICE_PAYMENT_DATA)
                .set(invoicePaymentRecord)
                .onConflict(INVOICE_PAYMENT_DATA.INVOICE_ID, INVOICE_PAYMENT_DATA.PAYMENT_ID)
                .doUpdate()
                .set(invoicePaymentRecord)
                .returning(INVOICE_PAYMENT_DATA.ID);

        KeyHolder keyHolder = new GeneratedKeyHolder();
        execute(query, keyHolder);
        return keyHolder.getKey() != null ? keyHolder.getKey().longValue() : null;
    }

}
