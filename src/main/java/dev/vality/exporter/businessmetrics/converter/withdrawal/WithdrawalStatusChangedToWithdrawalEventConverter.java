package dev.vality.exporter.businessmetrics.converter.withdrawal;

import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class WithdrawalStatusChangedToWithdrawalEventConverter implements WithdrawalEventConverter {

    @Override
    public WithdrawalEvent convert(MachineEvent machineEvent, TimestampedChange change) {
        var withdrawalStatusChanged = change.getChange().getStatusChanged();
        var status = withdrawalStatusChanged.getStatus();
        WithdrawalEvent event = new WithdrawalEvent();
        event.setWithdrawalId(machineEvent.getSourceId());
        event.setStatus(status.getSetField().getFieldName());
        event.setCreatedAt(Instant.parse(machineEvent.getCreatedAt()));
        return event;
    }

    @Override
    public boolean isApplicable(TimestampedChange change) {
        return change.getChange().isSetStatusChanged();
    }
}
