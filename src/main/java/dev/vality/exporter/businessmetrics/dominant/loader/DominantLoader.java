package dev.vality.exporter.businessmetrics.dominant.loader;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Slf4j
public abstract class DominantLoader<K> implements AsyncCacheLoader<K, String> {

    @Override
    public CompletableFuture<String> asyncLoad(K key, Executor executor) {
        return CompletableFuture.supplyAsync(
                () -> load(key),
                executor
        );
    }

    @Override
    public CompletableFuture<String> asyncReload(
            K key,
            String oldValue,
            Executor executor
    ) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return load(key);
            } catch (Exception e) {
                log.warn("Failed to refresh {} '{}'", logName(), key, e);
                return oldValue;
            }
        }, executor);
    }

    protected abstract String load(K key);

    protected abstract String logName();
}
