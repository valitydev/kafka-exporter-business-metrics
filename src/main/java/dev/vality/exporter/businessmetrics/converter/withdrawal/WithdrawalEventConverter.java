package dev.vality.exporter.businessmetrics.converter.withdrawal;

import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;


public interface WithdrawalEventConverter {

    WithdrawalEvent convert(MachineEvent machineEvent, TimestampedChange change);

    boolean isApplicable(TimestampedChange change);

}
