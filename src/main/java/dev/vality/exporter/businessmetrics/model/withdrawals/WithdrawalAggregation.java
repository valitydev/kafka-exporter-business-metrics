package dev.vality.exporter.businessmetrics.model.withdrawals;

import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WithdrawalAggregation {
    private long count;
    private long amount;
    private Instant lastUpdated;

    public WithdrawalAggregation add(WithdrawalEvent event) {
        return new WithdrawalAggregation(
                this.count + 1,
                this.amount + event.getAmount(),
                this.lastUpdated = event.getCreatedAt()
        );
    }
}
