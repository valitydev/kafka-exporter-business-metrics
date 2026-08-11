package dev.vality.exporter.businessmetrics.config;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import dev.vality.damsel.domain_config_v2.RepositoryClientSrv;
import dev.vality.exporter.businessmetrics.config.properties.DominantCacheProperties;
import dev.vality.exporter.businessmetrics.dominant.loader.*;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
@RequiredArgsConstructor
public class CacheConfig {

    private final DominantCacheProperties cacheProperties;

    @Bean
    public AsyncLoadingCache<Integer, String> providersCache(RepositoryClientSrv.Iface client) {
        return Caffeine.newBuilder()
                .maximumSize(cacheProperties.getProviders().getPoolSize())
                .refreshAfterWrite(
                        cacheProperties.getProviders().getTtlSec(),
                        TimeUnit.SECONDS
                )
                .buildAsync(new ProviderLoader(client));
    }

    @Bean
    public AsyncLoadingCache<Integer, String> terminalsCache(RepositoryClientSrv.Iface client) {
        return Caffeine.newBuilder()
                .maximumSize(cacheProperties.getTerminals().getPoolSize())
                .refreshAfterWrite(
                        cacheProperties.getTerminals().getTtlSec(),
                        TimeUnit.SECONDS
                )
                .buildAsync(new TerminalLoader(client));
    }

    @Bean
    public AsyncLoadingCache<String, String> shopsCache(
            RepositoryClientSrv.Iface client
    ) {
        return Caffeine.newBuilder()
                .maximumSize(cacheProperties.getShops().getPoolSize())
                .refreshAfterWrite(
                        cacheProperties.getShops().getTtlSec(),
                        TimeUnit.SECONDS
                )
                .buildAsync(new ShopLoader(client));
    }

    @Bean
    public AsyncLoadingCache<String, String> partiesCache(
            RepositoryClientSrv.Iface client
    ) {
        return Caffeine.newBuilder()
                .maximumSize(cacheProperties.getParties().getPoolSize())
                .refreshAfterWrite(
                        cacheProperties.getParties().getTtlSec(),
                        TimeUnit.SECONDS
                )
                .buildAsync(new PartyLoader(client));
    }

    @Bean
    public AsyncLoadingCache<String, String> walletsCache(
            RepositoryClientSrv.Iface client
    ) {
        return Caffeine.newBuilder()
                .maximumSize(cacheProperties.getWallets().getPoolSize())
                .refreshAfterWrite(
                        cacheProperties.getWallets().getTtlSec(),
                        TimeUnit.SECONDS
                )
                .buildAsync(new WalletLoader(client));
    }
}
