package dev.vality.exporter.businessmetrics;

import dev.vality.exporter.businessmetrics.config.TestMetricsConfig;
import dev.vality.exporter.businessmetrics.model.Metric;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.exporter.businessmetrics.service.MetricsService;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.Properties;

import static dev.vality.exporter.businessmetrics.utils.TestData.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@Import(TestMetricsConfig.class)
class MetricsTopologyIntegrationTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, SinkEvent> inputTopic;

    @Autowired
    private StreamsBuilder testBuilder;

    @Autowired
    private Serde<SinkEvent> sinkEventSerde;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private MetricsService metricsService;

    private static final String TEST_INVOICE_ID = "testInvoiceId";

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:8080");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        testDriver = new TopologyTestDriver(testBuilder.build(), props);
        inputTopic = testDriver.createInputTopic(
                "invoicing-events",
                Serdes.String().serializer(),
                sinkEventSerde.serializer()
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testPaymentEventFlow() {
        MachineEvent startedInvoicePaymentEvents = getStartedInvoicePaymentEvents(TEST_INVOICE_ID);
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(startedInvoicePaymentEvents);
        inputTopic.pipeInput(sinkEvent.getEvent().getSourceId(), sinkEvent);
        metricsService.export();
        ReadOnlyKeyValueStore<String, PaymentEvent> store =
                testDriver.getKeyValueStore("payment-started-store");
        PaymentEvent result = store.get(TEST_INVOICE_ID);
        assertNotNull(result);
        assertEquals(TEST_INVOICE_ID, result.getInvoiceId());
        Gauge gaugeCaptured = getGaugeCaptured();
        Assertions.assertNull(gaugeCaptured);
        Gauge gaugePending = getGaugePending();
        assertNotNull(gaugePending);
        assertEquals(1.0, gaugePending.value());

        MachineEvent routeInvoicePaymentEvents = getRouteChangedInvoicePaymentEvents(TEST_INVOICE_ID);
        sinkEvent.setEvent(routeInvoicePaymentEvents);
        inputTopic.pipeInput(sinkEvent.getEvent().getSourceId(), sinkEvent);
        metricsService.export();
        store =
                testDriver.getKeyValueStore("payment-route-store");
        result = store.get(TEST_INVOICE_ID);
        assertNotNull(result);
        assertEquals(TEST_INVOICE_ID, result.getInvoiceId());
        gaugeCaptured = getGaugeCaptured();
        Assertions.assertNull(gaugeCaptured);
        gaugePending = getGaugePending();
        assertNotNull(gaugePending);
        assertEquals(1.0, gaugePending.value());

        MachineEvent statusChangedPaymentEvents = getStatusChangedPaymentEvents(TEST_INVOICE_ID);
        sinkEvent.setEvent(statusChangedPaymentEvents);
        inputTopic.pipeInput(sinkEvent.getEvent().getSourceId(), sinkEvent);
        metricsService.export();

        store =
                testDriver.getKeyValueStore("payment-status-store");
        result = store.get(TEST_INVOICE_ID);
        assertNotNull(result);
        assertEquals(TEST_INVOICE_ID, result.getInvoiceId());
        gaugeCaptured = getGaugeCaptured();
        assertNotNull(gaugeCaptured);
        assertEquals(1.0, gaugeCaptured.value());
        gaugePending = getGaugePending();
        assertNotNull(gaugePending);
        assertEquals(1.0, gaugePending.value());

    }

    private Gauge getGaugePending() {
        return meterRegistry
                .find(Metric.PAYMENTS_STATUS_COUNT.getName())
                .tags(
                        "provider_id", "29",
                        "terminal_id", "30",
                        "shop_id", "test_shop_id",
                        "currency", "RUB",
                        "status", "pending",
                        "duration", "5m"
                )
                .gauge();
    }

    private Gauge getGaugeCaptured() {
        return meterRegistry
                .find(Metric.PAYMENTS_STATUS_COUNT.getName())
                .tags(
                        "provider_id", "21",
                        "terminal_id", "35",
                        "shop_id", "test_shop_id",
                        "currency", "RUB",
                        "status", "captured",
                        "duration", "15m"
                )
                .gauge();
    }

}
