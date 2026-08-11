package dev.vality.exporter.businessmetrics.dominant;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import dev.vality.damsel.domain.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class DominantCacheService {

    private final AsyncLoadingCache<Integer, String> terminalsCache;

    private final AsyncLoadingCache<String, String> shopsCache;

    private final AsyncLoadingCache<Integer, String> providersCache;

    private final AsyncLoadingCache<String, String> partiesCache;

    private final AsyncLoadingCache<String, String> walletsCache;

    public String getProviderName(ProviderRef ref) {
        try {
            return providersCache.get(ref.getId()).join();
        } catch (CompletionException e) {
            log.warn("Cannot resolve provider {}", ref.getId(), e);
            return "unknown";
        }
    }

    public String getTerminalName(TerminalRef ref) {
        try {
            return terminalsCache.get(ref.getId()).join();
        } catch (CompletionException e) {
            log.warn("Cannot resolve terminal {}", ref.getId(), e);
            return "unknown";
        }
    }

    public String getShopName(ShopConfigRef ref) {
        try {
            return shopsCache.get(ref.getId()).join();
        } catch (CompletionException e) {
            log.warn("Cannot resolve shop {}", ref.getId(), e);
            return "unknown";
        }
    }

    public String getPartyName(PartyConfigRef ref) {
        try {
            return partiesCache.get(ref.getId()).join();
        } catch (CompletionException e) {
            log.warn("Cannot resolve party {}", ref.getId(), e);
            return "unknown";
        }
    }

    public String getWalletName(WalletConfigRef ref) {
        try {
            return walletsCache.get(ref.getId()).join();
        } catch (CompletionException e) {
            log.warn("Cannot resolve wallet {}", ref.getId(), e);
            return "unknown";
        }
    }
}