package dev.vality.exporter.businessmetrics.model.payments;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentAggregation {
    private long count;
    private long amount;
    private Instant lastUpdated;

    public PaymentAggregation add(PaymentEvent event) {
        return new PaymentAggregation(
                this.count + 1,
                this.amount + event.getAmount(),
                this.lastUpdated = event.getCreatedAt()
        );
    }
}
