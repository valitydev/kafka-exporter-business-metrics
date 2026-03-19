package dev.vality.exporter.businessmetrics.model.withdrawals;

import com.fasterxml.jackson.annotation.JsonCreator;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class WithdrawalMetricKey {
    private int providerId;
    private int terminalId;
    private String walletId;
    private String currencyCode;
    private String status;

    @JsonCreator
    public static WithdrawalMetricKey from(WithdrawalEvent event) {
        return new WithdrawalMetricKey(
                event.getProviderId(),
                event.getTerminalId(),
                event.getWalletId(),
                event.getCurrencyCode(),
                event.getStatus()
        );
    }
}
