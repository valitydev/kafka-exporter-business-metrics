package dev.vality.exporter.businessmetrics.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WithdrawalTransactionMetricRow {
    private Integer providerId;
    private String providerName;
    private Integer terminalId;
    private String terminalName;
    private UUID partyId;
    private String partyName;
    private String walletId;
    private String walletName;
    private String currencyCode;
    private String currencyExponent;
    private Long count;
}
