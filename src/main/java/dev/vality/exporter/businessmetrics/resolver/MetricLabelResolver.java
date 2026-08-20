package dev.vality.exporter.businessmetrics.resolver;

import dev.vality.damsel.domain.*;
import dev.vality.exporter.businessmetrics.dominant.DominantCacheService;
import dev.vality.exporter.businessmetrics.dto.PaymentStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.PaymentTransactionMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalStatusMetricRow;
import dev.vality.exporter.businessmetrics.dto.WithdrawalTransactionMetricRow;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
public class MetricLabelResolver {

    private final DominantCacheService dominantCacheService;

    public PaymentStatusMetricRow resolve(PaymentStatusMetricRow row) {
        var providerName = dominantCacheService.getProviderName(new ProviderRef(row.getProviderId()));
        var terminalName = dominantCacheService.getTerminalName(new TerminalRef(row.getTerminalId()));
        var shopName = dominantCacheService.getShopName(new ShopConfigRef(row.getShopId()));
        var partyName = dominantCacheService.getPartyName(new PartyConfigRef(String.valueOf(row.getPartyId())));
        var currencyExponent = dominantCacheService.getCurrencyExponent(row.getCurrencyCode());
        CompletableFuture.allOf(providerName, terminalName, shopName, partyName, currencyExponent).join();
        row.setProviderName(providerName.join());
        row.setTerminalName(terminalName.join());
        row.setShopName(shopName.join());
        row.setPartyName(partyName.join());
        row.setCurrencyExponent(currencyExponent.join());
        return row;
    }

    public PaymentTransactionMetricRow resolve(PaymentTransactionMetricRow row) {
        var providerName = dominantCacheService.getProviderName(new ProviderRef(row.getProviderId()));
        var terminalName = dominantCacheService.getTerminalName(new TerminalRef(row.getTerminalId()));
        var shopName = dominantCacheService.getShopName(new ShopConfigRef(row.getShopId()));
        var partyName = dominantCacheService.getPartyName(new PartyConfigRef(String.valueOf(row.getPartyId())));
        var currencyExponent = dominantCacheService.getCurrencyExponent(row.getCurrencyCode());
        CompletableFuture.allOf(providerName, terminalName, shopName, partyName, currencyExponent).join();
        row.setProviderName(providerName.join());
        row.setTerminalName(terminalName.join());
        row.setShopName(shopName.join());
        row.setPartyName(partyName.join());
        row.setCurrencyExponent(currencyExponent.join());
        return row;
    }

    public WithdrawalStatusMetricRow resolve(WithdrawalStatusMetricRow row) {
        var providerName = dominantCacheService.getProviderName(new ProviderRef(row.getProviderId()));
        var terminalName = dominantCacheService.getTerminalName(new TerminalRef(row.getTerminalId()));
        var partyName = dominantCacheService.getPartyName(new PartyConfigRef(String.valueOf(row.getPartyId())));
        var walletName = dominantCacheService.getWalletName(new WalletConfigRef(String.valueOf(row.getWalletId())));
        var currencyExponent = dominantCacheService.getCurrencyExponent(row.getCurrencyCode());
        CompletableFuture.allOf(providerName, terminalName, walletName, partyName, currencyExponent).join();
        row.setProviderName(providerName.join());
        row.setTerminalName(terminalName.join());
        row.setPartyName(partyName.join());
        row.setWalletName(walletName.join());
        row.setCurrencyExponent(currencyExponent.join());
        return row;
    }

    public WithdrawalTransactionMetricRow resolve(WithdrawalTransactionMetricRow row) {
        var providerName = dominantCacheService.getProviderName(new ProviderRef(row.getProviderId()));
        var terminalName = dominantCacheService.getTerminalName(new TerminalRef(row.getTerminalId()));
        var walletName = dominantCacheService.getWalletName(new WalletConfigRef(String.valueOf(row.getWalletId())));
        var partyName = dominantCacheService.getPartyName(new PartyConfigRef(String.valueOf(row.getPartyId())));
        var currencyExponent = dominantCacheService.getCurrencyExponent(row.getCurrencyCode());
        CompletableFuture.allOf(providerName, terminalName, walletName, partyName, currencyExponent).join();
        row.setProviderName(providerName.join());
        row.setTerminalName(terminalName.join());
        row.setWalletName(walletName.join());
        row.setPartyName(partyName.join());
        row.setCurrencyExponent(currencyExponent.join());
        return row;
    }
}