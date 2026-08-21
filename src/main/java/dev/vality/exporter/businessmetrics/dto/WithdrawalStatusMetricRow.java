package dev.vality.exporter.businessmetrics.dto;

import dev.vality.exporter.businessmetrics.domain.enums.WithdrawalStatus;
import dev.vality.exporter.businessmetrics.model.TimeWindowMetrics;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WithdrawalStatusMetricRow {

    private Integer providerId;
    private Integer terminalId;
    private String providerName;
    private String terminalName;

    private UUID partyId;
    private String partyName;

    private String walletId;
    private String walletName;

    private String currencyCode;
    private String currencyExponent;

    private WithdrawalStatus withdrawalStatus;

    private TimeWindowMetrics metrics;
}
