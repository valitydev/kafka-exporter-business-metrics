package dev.vality.exporter.businessmetrics.converter.withdrawal;

import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class WithdrawalStartedToWithdrawalEventConverter implements WithdrawalEventConverter {

    @Override
    public WithdrawalEvent convert(MachineEvent machineEvent, TimestampedChange change) {
        var withdrawalStarted = change.getChange().getCreated();
        var withdrawal = withdrawalStarted.getWithdrawal();
        WithdrawalEvent event = new WithdrawalEvent();
        event.setWithdrawalId(machineEvent.getSourceId());
        if (withdrawal.isSetRoute()) {
            event.setProviderId(withdrawal.getRoute().getProviderId());
            event.setTerminalId(withdrawal.getRoute().getTerminalId());
        }
        event.setWalletId(withdrawal.getWalletId());
        event.setCurrencyCode(withdrawal.getBody().getCurrency().getSymbolicCode());
        event.setAmount(withdrawal.getBody().getAmount());
        event.setCreatedAt(Instant.parse(machineEvent.getCreatedAt()));
        return event;
    }

    @Override
    public boolean isApplicable(TimestampedChange change) {
        return change.getChange().isSetCreated();
    }
}
