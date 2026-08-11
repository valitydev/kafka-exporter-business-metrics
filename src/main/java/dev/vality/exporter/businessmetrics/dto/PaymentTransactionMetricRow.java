package dev.vality.exporter.businessmetrics.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentTransactionMetricRow {
    private Integer providerId;
    private String providerName;
    private Integer terminalId;
    private String terminalName;
    private UUID partyId;
    private String partyName;
    private String shopId;
    private String shopName;
    private String currencyCode;
    private Long count;
}
