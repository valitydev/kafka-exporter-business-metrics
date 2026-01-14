package dev.vality.exporter.businessmetrics.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum Metric {

    PAYMENTS_STATUS_COUNT(
            formatWithPrefix("payments_status_count"),
            "Payments statuses count"),

    PAYMENTS_TRANSACTION_COUNT(
            formatWithPrefix("payments_transaction_count"),
            "Payments new transactions since last scrape"),

    WITHDRAWALS_STATUS_COUNT(
            formatWithPrefix("withdrawals_status_count"),
            "Withdrawals statuses count"),

    PAYMENTS_AMOUNT(
            formatWithPrefix("payments_amount"),
            "Payments amount since last scrape"),

    WITHDRAWALS_AMOUNT(
            formatWithPrefix("withdrawals_amount"),
            "Withdrawals amount since last scrape"),

    CURRENCY_EXPONENT(
            formatWithPrefix("currency_exponent"),
            "Currency exponent");

    @Getter
    private final String name;
    @Getter
    private final String description;

    private static String formatWithPrefix(String name) {
        return String.format("ebm_%s", name);
    }
}
