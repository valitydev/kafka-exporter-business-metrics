package dev.vality.exporter.businessmetrics.topology;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MetricsTopology {

    private final StreamsBuilder streamsBuilder;
    private final PaymentMetricsTopology paymentMetricsTopology;

    public void buildTopology() {
        paymentMetricsTopology.buildPaymentTopology(streamsBuilder);
    }
}