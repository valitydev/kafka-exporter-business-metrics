package dev.vality.exporter.businessmetrics.handler.withdrawal;

import dev.vality.exporter.businessmetrics.handler.EventHandler;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;

public interface WithdrawalEventHandler extends EventHandler<TimestampedChange, MachineEvent> {
}
