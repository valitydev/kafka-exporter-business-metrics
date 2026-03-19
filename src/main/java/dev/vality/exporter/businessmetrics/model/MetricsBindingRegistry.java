package dev.vality.exporter.businessmetrics.model;

import dev.vality.exporter.businessmetrics.factory.MetricsFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class MetricsBindingRegistry {

    private final MetricsFactory factory;

    public List<MetricsBinding<?, ?>> all() {
        return List.of(
                factory.payments(),
                factory.withdrawals()
        );
    }
}
