package dev.vality.exporter.businessmetrics.model.payments;

import lombok.Data;

import java.time.Instant;

@Data
public class PaymentEvent {
    private String invoiceId;
    private int providerId;
    private int terminalId;
    private String shopId;
    private String currencyCode;
    private String status;
    private long amount;
    private Instant createdAt;
}
