package dev.vality.exporter.businessmetrics.resolver;

import dev.vality.damsel.domain.*;
import dev.vality.exporter.businessmetrics.dominant.DominantCacheService;
import dev.vality.exporter.businessmetrics.dto.PaymentStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.PaymentTransactionMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalStatusMetricRow;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MetricLabelResolver {

    private final DominantCacheService dominantCacheService;

    public PaymentStatusMetricRow resolve(PaymentStatusMetricRow row) {
        row.setProviderName(
                dominantCacheService.getProviderName(new ProviderRef(row.getProviderId())));
        row.setTerminalName(
                dominantCacheService.getTerminalName(new TerminalRef(row.getTerminalId())));
        row.setShopName(
                dominantCacheService.getShopName(new ShopConfigRef(row.getShopId())));
        row.setPartyName(
                dominantCacheService.getPartyName(new PartyConfigRef(String.valueOf(row.getPartyId()))));
        return row;
    }

    public PaymentTransactionMetricRow resolve(PaymentTransactionMetricRow row) {
        row.setProviderName(
                dominantCacheService.getProviderName(new ProviderRef(row.getProviderId())));
        row.setTerminalName(
                dominantCacheService.getTerminalName(new TerminalRef(row.getTerminalId())));
        row.setShopName(
                dominantCacheService.getShopName(new ShopConfigRef(row.getShopId())));
        row.setPartyName(
                dominantCacheService.getPartyName(new PartyConfigRef(String.valueOf(row.getPartyId()))));
        return row;
    }

    public WithdrawalStatusMetricRow resolve(WithdrawalStatusMetricRow row) {
        row.setProviderName(
                dominantCacheService.getProviderName(new ProviderRef(row.getProviderId())));
        row.setTerminalName(
                dominantCacheService.getTerminalName(new TerminalRef(row.getTerminalId())));
        row.setPartyName(
                dominantCacheService.getPartyName(new PartyConfigRef(String.valueOf(row.getPartyId()))));
        row.setWalletName(
                dominantCacheService.getWalletName(new WalletConfigRef(String.valueOf(row.getWalletId())))
        );
        return row;
    }
}
