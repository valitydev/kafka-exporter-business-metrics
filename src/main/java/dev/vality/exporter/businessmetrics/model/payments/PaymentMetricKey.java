package dev.vality.exporter.businessmetrics.model.payments;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PaymentMetricKey {
    private int providerId;
    private int terminalId;
    private String shopId;
    private String currencyCode;
    private String status;

    @JsonCreator
    public static PaymentMetricKey from(PaymentEvent event) {
        return new PaymentMetricKey(
                event.getProviderId(),
                event.getTerminalId(),
                event.getShopId(),
                event.getCurrencyCode(),
                event.getStatus()
        );
    }
}


