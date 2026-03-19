package dev.vality.exporter.businessmetrics.config;

import dev.vality.exporter.businessmetrics.config.properties.MetricsTtlProperties;
import dev.vality.exporter.businessmetrics.factory.MetricsFactory;
import dev.vality.exporter.businessmetrics.model.MetricsBindingRegistry;
import dev.vality.exporter.businessmetrics.model.payments.PaymentMetricsStore;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalMetricsStore;
import dev.vality.exporter.businessmetrics.service.MetricsService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MetricsTtlProperties.class)
@RequiredArgsConstructor
public class MetricsConfig {

    @Bean
    @ConditionalOnMissingBean
    public MetricsFactory metricsFactory(
            MeterRegistry registry,
            PaymentMetricsStore paymentStore,
            WithdrawalMetricsStore withdrawalStore
    ) {
        return new MetricsFactory(registry, paymentStore, withdrawalStore);
    }

    @Bean
    @ConditionalOnMissingBean
    public MetricsBindingRegistry metricsBindingRegistry(
            MetricsFactory factory
    ) {
        return new MetricsBindingRegistry(factory);
    }

    @Bean
    @ConditionalOnMissingBean
    public MetricsService metricsService(
            MetricsBindingRegistry metricsBindingRegistry,
            MeterRegistry registry,
            MetricsTtlProperties ttlProperties
    ) {
        return new MetricsService(metricsBindingRegistry, registry, ttlProperties);
    }
}
