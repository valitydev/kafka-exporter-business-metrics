package dev.vality.exporter.businessmetrics.dao;

import dev.vality.dao.GenericDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;

public interface WithdrawalDao extends GenericDao {

    WithdrawalData get(String withdrawalId);

    Long save(WithdrawalData withdrawal);

}
