package dev.vality.exporter.businessmetrics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "service.dominant.cache")
public class DominantCacheProperties {

    private CacheConfig terminals;
    private CacheConfig providers;
    private CacheConfig shops;
    private CacheConfig parties;
    private CacheConfig wallets;
    private CacheConfig currencies;

    @Getter
    @Setter
    public static class CacheConfig {
        private int poolSize;
        private int ttlSec;
    }
}
