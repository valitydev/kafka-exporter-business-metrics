package dev.vality.exporter.businessmetrics.service;

import dev.vality.dao.DaoException;
import dev.vality.exporter.businessmetrics.dao.WithdrawalDao;
import dev.vality.exporter.businessmetrics.domain.enums.WithdrawalStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
import dev.vality.exporter.businessmetrics.exception.NotFoundException;
import dev.vality.fistful.base.EventRange;
import dev.vality.fistful.withdrawal.ManagementSrv;
import dev.vality.fistful.withdrawal.Route;
import dev.vality.fistful.withdrawal.WithdrawalState;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class WithdrawalClient {

    private final WithdrawalDao withdrawalDao;
    private final ManagementSrv.Iface managementClient;

    public WithdrawalData getWithdrawalData(MachineEvent event) throws DaoException {
        WithdrawalData withdrawalData = withdrawalDao.get(event.getSourceId());
        if (withdrawalData != null) {
            return withdrawalData;
        }
        return loadWithdrawalFromFistful(event);
    }

    @SneakyThrows
    private WithdrawalData loadWithdrawalFromFistful(MachineEvent event) {
        WithdrawalState withdrawalState =
                managementClient.get(
                        event.getSourceId(),
                        new EventRange().setLimit(Math.toIntExact(event.getEventId())));
        if (withdrawalState == null) {
            throw new NotFoundException(
                    String.format("WithdrawalEvent with withdrawalId='%s' not found", event.getSourceId()));
        }
        return convertStateToWithdrawalData(withdrawalState);
    }

    private WithdrawalData convertStateToWithdrawalData(WithdrawalState state) {
        var withdrawalData = new WithdrawalData();
        withdrawalData.setWithdrawalId(state.getId());
        withdrawalData.setWalletId(state.getWalletId());
        withdrawalData.setPartyId(UUID.fromString(state.getPartyId()));
        withdrawalData.setAmount(state.getBody().getAmount());
        withdrawalData.setCurrencyCode(state.getBody().getCurrency().getSymbolicCode());
        LocalDateTime createdAt = LocalDateTime.parse(state.getCreatedAt());
        withdrawalData.setCreatedAt(createdAt);
        withdrawalData.setWithdrawalStatus(TBaseUtil.unionFieldToEnum(state.getStatus(), WithdrawalStatus.class));
        Route route = state.getRoute();
        if (Objects.nonNull(route)) {
            withdrawalData.setProviderId(route.getProviderId());
            withdrawalData.setTerminalId(route.getTerminalId());
        }
        return withdrawalData;
    }
}
