package dev.vality.exporter.businessmetrics.converter.withdrawal;

import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class WithdrawalRouteChangedToWithdrawalEventConverter implements WithdrawalEventConverter {

    @Override
    public WithdrawalEvent convert(MachineEvent machineEvent, TimestampedChange change) {
        var withdrawalRouteChanged = change.getChange().getRoute();
        var route = withdrawalRouteChanged.getRoute();
        WithdrawalEvent event = new WithdrawalEvent();
        event.setWithdrawalId(machineEvent.getSourceId());
        event.setProviderId(route.getProviderId());
        event.setTerminalId(route.getTerminalId());
        event.setCreatedAt(Instant.parse(machineEvent.getCreatedAt()));
        return event;
    }

    @Override
    public boolean isApplicable(TimestampedChange change) {
        return change.getChange().isSetRoute();
    }
}
