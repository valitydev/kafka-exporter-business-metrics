package dev.vality.exporter.businessmetrics.dominant;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import dev.vality.damsel.domain.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class DominantCacheService {

    private final AsyncLoadingCache<Integer, String> terminalsCache;

    private final AsyncLoadingCache<String, String> shopsCache;

    private final AsyncLoadingCache<Integer, String> providersCache;

    private final AsyncLoadingCache<String, String> partiesCache;

    private final AsyncLoadingCache<String, String> walletsCache;

    private final AsyncLoadingCache<String, String> currencyCache;

    private static final int UNKNOWN_ID = -1;
    private static final String UNKNOWN_VALUE = "unknown";

    public CompletableFuture<String> getProviderName(ProviderRef ref) {
        if (Objects.equals(ref.getId(),UNKNOWN_ID)) {
            return CompletableFuture.completedFuture(UNKNOWN_VALUE);
        }
        return get(providersCache, ref.getId(), "provider");
    }

    public CompletableFuture<String> getTerminalName(TerminalRef ref) {
        if (Objects.equals(ref.getId(),UNKNOWN_ID)) {
            return CompletableFuture.completedFuture(UNKNOWN_VALUE);
        }
        return get(terminalsCache, ref.getId(), "terminal");
    }

    public CompletableFuture<String> getShopName(ShopConfigRef ref) {
        return get(shopsCache, ref.getId(), "shop");
    }

    public CompletableFuture<String> getPartyName(PartyConfigRef ref) {
        return get(partiesCache, ref.getId(), "party");
    }

    public CompletableFuture<String> getWalletName(WalletConfigRef ref) {
        return get(walletsCache, ref.getId(), "wallet");
    }

    public CompletableFuture<String> getCurrencyExponent(String ref) {
        return get(currencyCache, ref, "currency");
    }

    private <K> CompletableFuture<String> get(
            AsyncLoadingCache<K, String> cache,
            K key,
            String entity
    ) {
        return cache.get(key)
                .exceptionally(e -> {
                    log.warn("Cannot resolve {} {}", entity, key, e);
                    return UNKNOWN_VALUE;
                });
    }
}