package dev.vality.exporter.businessmetrics.converter.withdrawal;

import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class WithdrawalEventConverterHandler {

    private final List<WithdrawalEventConverter> converters;

    public List<WithdrawalEvent> handle(MachineEvent event, TimestampedChange change) {
        if (change.isSetChange()) {
            return converters.stream()
                    .filter(c -> c.isApplicable(change))
                    .map(c -> c.convert(event, change))
                    .toList();
        }
        return new ArrayList<>();
    }
}
