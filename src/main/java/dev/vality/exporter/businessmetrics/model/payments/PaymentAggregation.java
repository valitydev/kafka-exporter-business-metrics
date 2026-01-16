package dev.vality.exporter.businessmetrics.model.payments;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentAggregation {
    private long count;
    private long amount;

    public PaymentAggregation add(PaymentEvent event) {
        return new PaymentAggregation(
                this.count + 1,
                this.amount + event.getAmount()
        );
    }
}
