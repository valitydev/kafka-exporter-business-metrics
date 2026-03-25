package dev.vality.exporter.businessmetrics.model.withdrawals;

import lombok.Data;

import java.time.Instant;

@Data
public class WithdrawalEvent {
    private String withdrawalId;
    private int providerId;
    private int terminalId;
    private String walletId;
    private String currencyCode;
    private String status;
    private long amount;
    private Instant createdAt;
}
