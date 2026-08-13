package dev.vality.exporter.businessmetrics.service;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import dev.vality.damsel.domain.ProviderRef;
import dev.vality.damsel.domain.ShopConfigRef;
import dev.vality.damsel.domain.TerminalRef;
import dev.vality.exporter.businessmetrics.config.PostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.dominant.DominantCacheService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@PostgresqlSpringBootITest
public class DominantCacheServiceTest {

    @MockitoBean
    private AsyncLoadingCache<Integer, String> providersCache;

    @MockitoBean
    private AsyncLoadingCache<Integer, String> terminalsCache;

    @MockitoBean
    private AsyncLoadingCache<String, String> shopsCache;

    @Autowired
    private DominantCacheService service;

    @Test
    void shouldReturnProviderNameFromCache() {

        when(providersCache.get(21))
                .thenReturn(CompletableFuture.completedFuture("Provider"));

        String result =
                service.getProviderName(new ProviderRef(21));

        assertThat(result).isEqualTo("Provider");

        verify(providersCache).get(21);
    }

    @Test
    void shouldReturnTerminalNameFromCache() {

        when(terminalsCache.get(35))
                .thenReturn(CompletableFuture.completedFuture("Terminal"));

        String result =
                service.getTerminalName(new TerminalRef(35));

        assertThat(result).isEqualTo("Terminal");
    }

    @Test
    void shouldReturnShopNameFromCache() {

        when(shopsCache.get("shop-1"))
                .thenReturn(
                        CompletableFuture.completedFuture("Shop")
                );

        String result =
                service.getShopName(new ShopConfigRef("shop-1"));

        assertThat(result).isEqualTo("Shop");
    }

    @Test
    void shouldReturnUnknownWhenProviderLoadingFails() {

        CompletableFuture<String> future =
                new CompletableFuture<>();

        future.completeExceptionally(
                new RuntimeException("Dominant unavailable")
        );

        when(providersCache.get(21))
                .thenReturn(future);

        String result =
                service.getProviderName(new ProviderRef(21));

        assertThat(result).isEqualTo("unknown");
    }

    @Test
    void shouldReturnUnknownWhenTerminalLoadingFails() {

        CompletableFuture<String> future =
                new CompletableFuture<>();

        future.completeExceptionally(
                new RuntimeException("Dominant unavailable")
        );

        when(terminalsCache.get(35))
                .thenReturn(future);

        assertThat(
                service.getTerminalName(new TerminalRef(35))
        ).isEqualTo("unknown");
    }
}
