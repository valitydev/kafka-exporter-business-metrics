package dev.vality.exporter.businessmetrics.handler.withdrawal;

import dev.vality.dao.DaoException;
import dev.vality.exporter.businessmetrics.dao.WithdrawalDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
import dev.vality.exporter.businessmetrics.exception.NotFoundException;
import dev.vality.exporter.businessmetrics.exception.StorageException;
import dev.vality.fistful.withdrawal.Route;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalRouteChangedEventHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetRoute()
                && change.getChange().getRoute().isSetRoute();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle WithdrawalRouteChanged: eventId={}, withdrawalId={}", event.getEventId(),
                    event.getSourceId());
            WithdrawalData withdrawalData = getWithdrawalData(event);
            Route route = change.getChange().getRoute().getRoute();
            if (Objects.nonNull(route)) {
                withdrawalData.setProviderId(route.getProviderId());
                withdrawalData.setTerminalId(route.getTerminalId());
            }
            Long id = withdrawalDao.save(withdrawalData);
            log.info("WithdrawalRouteChanged has {} been saved: eventId={}, withdrawalId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    private WithdrawalData getWithdrawalData(MachineEvent event) throws DaoException {
        WithdrawalData withdrawalData = withdrawalDao.get(event.getSourceId());

        if (withdrawalData == null) {
            throw new NotFoundException(
                    String.format("WithdrawalEvent with withdrawalId='%s' not found", event.getSourceId()));
        }

        return withdrawalData;
    }
}
