package dev.vality.exporter.businessmetrics.factory;

import dev.vality.exporter.businessmetrics.dto.PaymentStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.PaymentTransactionMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalStatusMetricRow;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;

@Component
public class MetricTagsFactory {


    public Tags paymentTags(
            PaymentStatusMetricRow row,
            String window
    ) {

        return Tags.of(

                "provider_id",
                String.valueOf(row.getProviderId()),

                "provider_name",
                row.getProviderName(),

                "terminal_id",
                String.valueOf(row.getTerminalId()),

                "terminal_name",
                row.getTerminalName(),

                "party_id",
                String.valueOf(row.getPartyId()),

                "party_name",
                row.getPartyName(),

                "shop_id",
                row.getShopId(),

                "shop_name",
                row.getShopName(),

                "currency",
                row.getCurrencyCode(),

                "status",
                row.getPaymentStatus().name(),

                "window",
                window
        );
    }


    public Tags transactionTags(
            PaymentTransactionMetricRow row
    ) {

        return Tags.of(

                "provider_id",
                String.valueOf(row.getProviderId()),

                "provider_name",
                row.getProviderName(),

                "terminal_id",
                String.valueOf(row.getTerminalId()),

                "terminal_name",
                row.getTerminalName(),

                "party_id",
                String.valueOf(row.getPartyId()),

                "party_name",
                row.getPartyName(),

                "shop_id",
                row.getShopId(),

                "shop_name",
                row.getShopName(),

                "currency",
                row.getCurrencyCode()
        );
    }


    public Tags withdrawalTags(
            WithdrawalStatusMetricRow row,
            String window
    ) {

        return Tags.of(

                "provider_id",
                String.valueOf(row.getProviderId()),

                "provider_name",
                row.getProviderName(),

                "terminal_id",
                String.valueOf(row.getTerminalId()),

                "terminal_name",
                row.getTerminalName(),

                "party_id",
                String.valueOf(row.getPartyId()),

                "party_name",
                row.getPartyName(),

                "wallet_id",
                row.getWalletId(),

                "wallet_name",
                row.getWalletName(),

                "currency",
                row.getCurrencyCode(),

                "status",
                row.getWithdrawalStatus().name(),

                "window",
                window
        );
    }
}
