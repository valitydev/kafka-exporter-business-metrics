package dev.vality.exporter.businessmetrics.binding;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class MetricsBindingRegistry {

    private final MetricsBindingFactory factory;

    public List<MetricsBinding<?, ?>> all() {
        return List.of(
                factory.payments(),
                factory.withdrawals()
        );
    }
}
