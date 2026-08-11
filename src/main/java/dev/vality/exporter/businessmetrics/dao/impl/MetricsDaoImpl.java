package dev.vality.exporter.businessmetrics.dao.impl;

import dev.vality.exporter.businessmetrics.dao.MetricsDao;
import dev.vality.exporter.businessmetrics.dto.PaymentStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.PaymentTransactionMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalStatusMetricRow;
import dev.vality.exporter.businessmetrics.model.MetricWindow;
import dev.vality.exporter.businessmetrics.model.TimeWindowMetrics;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static dev.vality.exporter.businessmetrics.domain.Tables.INVOICE_PAYMENT_DATA;
import static dev.vality.exporter.businessmetrics.domain.Tables.WITHDRAWAL_DATA;
import static org.jooq.impl.DSL.count;

@Component
@RequiredArgsConstructor
public class MetricsDaoImpl implements MetricsDao {

    private static final ZoneId MOSCOW_ZONE = ZoneId.of("Europe/Moscow");

    private final DSLContext dsl;

    @Override
    public List<PaymentStatusMetricRow> getPaymentStatusMetrics() {
        LocalDateTime now = LocalDateTime.now();

        SelectFieldOrAsterisk[] metricFields = createMetricFields(
                INVOICE_PAYMENT_DATA.CREATED_AT,
                INVOICE_PAYMENT_DATA.AMOUNT,
                now);
        return dsl.select(
                        INVOICE_PAYMENT_DATA.PROVIDER_ID,
                        INVOICE_PAYMENT_DATA.TERMINAL_ID,
                        INVOICE_PAYMENT_DATA.PARTY_ID,
                        INVOICE_PAYMENT_DATA.SHOP_ID,
                        INVOICE_PAYMENT_DATA.CURRENCY_CODE,
                        INVOICE_PAYMENT_DATA.PAYMENT_STATUS)
                .select(metricFields)
                .from(INVOICE_PAYMENT_DATA)
                .where(INVOICE_PAYMENT_DATA.CREATED_AT.gt(now.minusHours(24)))
                .groupBy(INVOICE_PAYMENT_DATA.PROVIDER_ID,
                        INVOICE_PAYMENT_DATA.TERMINAL_ID,
                        INVOICE_PAYMENT_DATA.PARTY_ID,
                        INVOICE_PAYMENT_DATA.SHOP_ID,
                        INVOICE_PAYMENT_DATA.CURRENCY_CODE,
                        INVOICE_PAYMENT_DATA.PAYMENT_STATUS)
                .fetch().map(this::mapPaymentStatusMetricRow);
    }

    @Override
    public List<PaymentTransactionMetricRow> getPaymentTransactionMetrics(LocalDateTime from) {
        return dsl.select(
                        INVOICE_PAYMENT_DATA.PROVIDER_ID,
                        INVOICE_PAYMENT_DATA.TERMINAL_ID,
                        INVOICE_PAYMENT_DATA.PARTY_ID,
                        INVOICE_PAYMENT_DATA.SHOP_ID,
                        INVOICE_PAYMENT_DATA.CURRENCY_CODE,
                        count().as("count"))
                .from(INVOICE_PAYMENT_DATA)
                .where(INVOICE_PAYMENT_DATA.CREATED_AT.gt(from))
                .groupBy(INVOICE_PAYMENT_DATA.PROVIDER_ID,
                        INVOICE_PAYMENT_DATA.TERMINAL_ID,
                        INVOICE_PAYMENT_DATA.PARTY_ID,
                        INVOICE_PAYMENT_DATA.SHOP_ID,
                        INVOICE_PAYMENT_DATA.CURRENCY_CODE)
                .fetchInto(PaymentTransactionMetricRow.class);
    }

    @Override
    public List<WithdrawalStatusMetricRow> getWithdrawalStatusMetrics() {
        LocalDateTime now = LocalDateTime.now();
        SelectFieldOrAsterisk[] metricFields = createMetricFields(
                WITHDRAWAL_DATA.CREATED_AT,
                WITHDRAWAL_DATA.AMOUNT,
                now);
        return dsl.select(
                        WITHDRAWAL_DATA.PROVIDER_ID,
                        WITHDRAWAL_DATA.TERMINAL_ID,
                        WITHDRAWAL_DATA.PARTY_ID,
                        WITHDRAWAL_DATA.WALLET_ID,
                        WITHDRAWAL_DATA.CURRENCY_CODE,
                        WITHDRAWAL_DATA.WITHDRAWAL_STATUS)
                .select(metricFields)
                .from(WITHDRAWAL_DATA)
                .where(WITHDRAWAL_DATA.CREATED_AT.gt(now.minusHours(24)))
                .groupBy(WITHDRAWAL_DATA.PROVIDER_ID,
                        WITHDRAWAL_DATA.TERMINAL_ID,
                        WITHDRAWAL_DATA.PARTY_ID,
                        WITHDRAWAL_DATA.WALLET_ID,
                        WITHDRAWAL_DATA.CURRENCY_CODE,
                        WITHDRAWAL_DATA.WITHDRAWAL_STATUS)
                .fetch().map(this::mapWithdrawalStatusMetricRow);
    }

    private SelectFieldOrAsterisk[] createMetricFields(Field<LocalDateTime> createdAt,
                                                       Field<Long> amount,
                                                       LocalDateTime now) {
        List<SelectFieldOrAsterisk> fields = new ArrayList<>();
        for (MetricWindow window : MetricWindow.values()) {
            LocalDateTime from = getWindowStart(window, now);
            fields.add(countField(createdAt, window, from));
            fields.add(amountField(createdAt, amount, window, from));
        }
        return fields.toArray(new SelectFieldOrAsterisk[0]);
    }

    private LocalDateTime getWindowStart(MetricWindow window, LocalDateTime now) {
        if (window == MetricWindow.TODAY_MSK) {
            return LocalDate.now(MOSCOW_ZONE).atStartOfDay();
        }
        return now.minus(window.getDuration());
    }

    private Field<Long> countField(Field<LocalDateTime> createdAt, MetricWindow window, LocalDateTime from) {
        return DSL.sum(
                        DSL.when(createdAt.gt(from),
                                        DSL.val(1L))
                                .otherwise(0L))
                .cast(Long.class)
                .as(countAlias(window));
    }

    private Field<Long> amountField(Field<LocalDateTime> createdAt,
                                    Field<Long> amount,
                                    MetricWindow window, LocalDateTime from) {
        return DSL.coalesce(
                        DSL.sum(
                                DSL.when(createdAt.gt(from), amount)
                                        .otherwise(0L)),
                        DSL.val(0L))
                .cast(Long.class)
                .as(amountAlias(window));
    }

    private String countAlias(MetricWindow window) {
        return "count" + aliasSuffix(window);
    }

    private String amountAlias(MetricWindow window) {
        return "amount" + aliasSuffix(window);
    }

    private String aliasSuffix(MetricWindow window) {
        if (window == MetricWindow.TODAY_MSK) {
            return "TodayMsk";
        }
        return window.getLabel();
    }

    private TimeWindowMetrics mapTimeWindowMetrics(org.jooq.Record record) {
        TimeWindowMetrics metrics = new TimeWindowMetrics();
        for (MetricWindow window : MetricWindow.values()) {
            Long count = record.get(countAlias(window), Long.class);
            Long amount = record.get(amountAlias(window), Long.class);
            metrics.put(window, count, amount);
        }
        return metrics;
    }

    private PaymentStatusMetricRow mapPaymentStatusMetricRow(org.jooq.Record record) {
        PaymentStatusMetricRow row = new PaymentStatusMetricRow();
        row.setProviderId(record.get(INVOICE_PAYMENT_DATA.PROVIDER_ID));
        row.setTerminalId(record.get(INVOICE_PAYMENT_DATA.TERMINAL_ID));
        row.setPartyId(record.get(INVOICE_PAYMENT_DATA.PARTY_ID));
        row.setShopId(record.get(INVOICE_PAYMENT_DATA.SHOP_ID));
        row.setCurrencyCode(record.get(INVOICE_PAYMENT_DATA.CURRENCY_CODE));
        row.setPaymentStatus(record.get(INVOICE_PAYMENT_DATA.PAYMENT_STATUS));
        row.setMetrics(mapTimeWindowMetrics(record));
        return row;
    }

    private WithdrawalStatusMetricRow mapWithdrawalStatusMetricRow(org.jooq.Record record) {
        WithdrawalStatusMetricRow row = new WithdrawalStatusMetricRow();
        row.setProviderId(record.get(WITHDRAWAL_DATA.PROVIDER_ID));
        row.setTerminalId(record.get(WITHDRAWAL_DATA.TERMINAL_ID));
        row.setPartyId(record.get(WITHDRAWAL_DATA.PARTY_ID));
        row.setWalletId(record.get(WITHDRAWAL_DATA.WALLET_ID));
        row.setCurrencyCode(record.get(WITHDRAWAL_DATA.CURRENCY_CODE));
        row.setWithdrawalStatus(record.get(WITHDRAWAL_DATA.WITHDRAWAL_STATUS));
        row.setMetrics(mapTimeWindowMetrics(record));
        return row;
    }
}