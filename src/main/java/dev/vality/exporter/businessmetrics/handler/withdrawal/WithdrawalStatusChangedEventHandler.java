package dev.vality.exporter.businessmetrics.handler.withdrawal;

import dev.vality.dao.DaoException;
import dev.vality.exporter.businessmetrics.dao.WithdrawalDao;
import dev.vality.exporter.businessmetrics.domain.enums.WithdrawalStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
import dev.vality.exporter.businessmetrics.exception.StorageException;
import dev.vality.exporter.businessmetrics.service.WithdrawalClient;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalStatusChangedEventHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;
    private final WithdrawalClient withdrawalClient;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetStatusChanged()
                && change.getChange().getStatusChanged().isSetStatus();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle WithdrawalStatusChanged: eventId={}, withdrawalId={}", event.getEventId(),
                    event.getSourceId());

            WithdrawalData withdrawalData = withdrawalClient.getWithdrawalData(event);
            Status status = change.getChange().getStatusChanged().getStatus();
            withdrawalData.setWithdrawalStatus(TBaseUtil.unionFieldToEnum(status, WithdrawalStatus.class));

            Long id = withdrawalDao.save(withdrawalData);

            log.info("WithdrawalStatusChanged has {} been saved: eventId={}, withdrawalId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }
}
