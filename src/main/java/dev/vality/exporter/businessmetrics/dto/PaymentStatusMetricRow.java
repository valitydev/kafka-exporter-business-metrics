package dev.vality.exporter.businessmetrics.dto;

import dev.vality.exporter.businessmetrics.domain.enums.InvoicePaymentStatus;
import dev.vality.exporter.businessmetrics.model.TimeWindowMetrics;
import lombok.Data;

import java.util.UUID;

@Data
public class PaymentStatusMetricRow {

    private Integer providerId;
    private Integer terminalId;
    private String providerName;
    private String terminalName;

    private UUID partyId;
    private String partyName;
    private String shopId;
    private String shopName;

    private String currencyCode;

    private InvoicePaymentStatus paymentStatus;

    private TimeWindowMetrics metrics;
}
