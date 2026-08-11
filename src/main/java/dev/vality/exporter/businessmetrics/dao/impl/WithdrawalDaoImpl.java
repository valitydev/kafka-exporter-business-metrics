package dev.vality.exporter.businessmetrics.dao.impl;

import dev.vality.dao.DaoException;
import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.exporter.businessmetrics.dao.WithdrawalDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
import dev.vality.exporter.businessmetrics.domain.tables.records.WithdrawalDataRecord;
import dev.vality.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static dev.vality.exporter.businessmetrics.domain.tables.WithdrawalData.WITHDRAWAL_DATA;

@Component
public class WithdrawalDaoImpl extends AbstractGenericDao implements WithdrawalDao {

    private final RecordRowMapper<WithdrawalData> withdrawalRecordRowMapper;

    public WithdrawalDaoImpl(DataSource dataSource) {
        super(dataSource);
        withdrawalRecordRowMapper = new RecordRowMapper<>(WITHDRAWAL_DATA, WithdrawalData.class);
    }

    @Override
    public WithdrawalData get(String withdrawalId) throws DaoException {
        Query query = getDslContext()
                .selectFrom(WITHDRAWAL_DATA)
                .where(WITHDRAWAL_DATA.WITHDRAWAL_ID.eq(withdrawalId));

        return fetchOne(query, withdrawalRecordRowMapper);
    }

    @Override
    public Long save(WithdrawalData withdrawal) throws DaoException {
        WithdrawalDataRecord withdrawalRecord = getDslContext().newRecord(WITHDRAWAL_DATA, withdrawal);

        Query query = getDslContext().insertInto(WITHDRAWAL_DATA)
                .set(withdrawalRecord)
                .onConflict(WITHDRAWAL_DATA.WITHDRAWAL_ID)
                .doUpdate()
                .set(withdrawalRecord)
                .returning(WITHDRAWAL_DATA.ID);

        KeyHolder keyHolder = new GeneratedKeyHolder();
        execute(query, keyHolder);
        return keyHolder.getKey() != null ? keyHolder.getKey().longValue() : null;
    }

}
