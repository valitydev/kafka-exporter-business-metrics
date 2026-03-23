package dev.vality.exporter.businessmetrics;

import dev.vality.exporter.businessmetrics.config.TestMetricsConfig;
import dev.vality.exporter.businessmetrics.model.Metric;
import dev.vality.exporter.businessmetrics.model.payments.PaymentEvent;
import dev.vality.exporter.businessmetrics.model.withdrawals.WithdrawalEvent;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.Collection;
import java.util.Properties;

import static dev.vality.exporter.businessmetrics.utils.TestData.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Import(TestMetricsConfig.class)
class MetricsTopologyIntegrationTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, SinkEvent> inputPaymentTopic;
    private TestInputTopic<String, SinkEvent> inputWithdrawalTopic;

    @Autowired
    private StreamsBuilder testBuilder;

    @Autowired
    private Serde<SinkEvent> sinkEventSerde;

    @Autowired
    private MeterRegistry meterRegistry;

    private static final String TEST_INVOICE_ID = "testInvoiceId";
    private static final String TEST_WITHDRAWAL_ID = "testWithdrawawalId";

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:8080");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");
        testDriver = new TopologyTestDriver(testBuilder.build(), props);
        inputPaymentTopic = testDriver.createInputTopic(
                "invoicing-events",
                Serdes.String().serializer(),
                sinkEventSerde.serializer()
        );
        inputWithdrawalTopic = testDriver.createInputTopic(
                "withdrawal-events",
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
        inputPaymentTopic.pipeInput(sinkEvent.getEvent().getSourceId(), sinkEvent);
        meterRegistry.getMeters().forEach(meter -> {
            if (meter instanceof Gauge gauge) {
                gauge.value();
            }
        });
        ReadOnlyKeyValueStore<String, PaymentEvent> store =
                testDriver.getKeyValueStore("payment-started-store");
        PaymentEvent result = store.get(TEST_INVOICE_ID);
        assertNotNull(result);
        assertEquals(TEST_INVOICE_ID, result.getInvoiceId());
        Gauge gaugeCaptured = getGaugePaymentCaptured();
        assertNull(gaugeCaptured);
        Gauge gaugePending = getGaugePaymentPending();
        assertNull(gaugePending);

        MachineEvent routeInvoicePaymentEvents = getRouteChangedInvoicePaymentEvents(TEST_INVOICE_ID);
        sinkEvent.setEvent(routeInvoicePaymentEvents);
        inputPaymentTopic.pipeInput(sinkEvent.getEvent().getSourceId(), sinkEvent);
        meterRegistry.getMeters().forEach(meter -> {
            if (meter instanceof Gauge gauge) {
                gauge.value();
            }
        });
        store =
                testDriver.getKeyValueStore("payment-route-store");
        result = store.get(TEST_INVOICE_ID);
        assertNotNull(result);
        assertEquals(TEST_INVOICE_ID, result.getInvoiceId());
        gaugeCaptured = getGaugePaymentCaptured();
        assertNull(gaugeCaptured);
        gaugePending = getGaugePaymentPendingWithRoute();
        assertNotNull(gaugePending);

        MachineEvent statusChangedPaymentEvents = getStatusChangedPaymentEvents(TEST_INVOICE_ID);
        sinkEvent.setEvent(statusChangedPaymentEvents);
        inputPaymentTopic.pipeInput(sinkEvent.getEvent().getSourceId(), sinkEvent);
        meterRegistry.getMeters().forEach(meter -> {
            if (meter instanceof Gauge gauge) {
                gauge.value();
            }
        });
        store =
                testDriver.getKeyValueStore("payment-status-store");
        result = store.get(TEST_INVOICE_ID);
        assertNotNull(result);
        assertEquals(TEST_INVOICE_ID, result.getInvoiceId());
        gaugeCaptured = getGaugePaymentCaptured();
        assertNotNull(gaugeCaptured);
        assertEquals(1.0, gaugeCaptured.value());
        gaugePending = getGaugePaymentPending();
        assertNotNull(gaugePending);
        Collection<Meter> meters = getMeterPaymentCaptured();
        assertEquals(1, meters.size());
    }

    @Test
    void testWithdrawalEventFlow() {
        MachineEvent startedWithdrawalPaymentEvents = getStartedWithdrawalEvents(TEST_WITHDRAWAL_ID);
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(startedWithdrawalPaymentEvents);
        inputWithdrawalTopic.pipeInput(sinkEvent.getEvent().getSourceId(), sinkEvent);
        meterRegistry.getMeters().forEach(meter -> {
            if (meter instanceof Gauge gauge) {
                gauge.value();
            }
        });
        ReadOnlyKeyValueStore<String, WithdrawalEvent> store =
                testDriver.getKeyValueStore("withdrawal-started-store");
        WithdrawalEvent result = store.get(TEST_WITHDRAWAL_ID);
        assertNotNull(result);
        assertEquals(TEST_WITHDRAWAL_ID, result.getWithdrawalId());
        Gauge gaugeCaptured = getGaugeWithdrawalSucceeded();
        assertNull(gaugeCaptured);
        Gauge gaugePending = getGaugeWithdrawalPending();
        assertNull(gaugePending);

        MachineEvent routeWithdrawalEvents = getRouteChangedWithdrawalEvents(TEST_WITHDRAWAL_ID);
        sinkEvent.setEvent(routeWithdrawalEvents);
        inputWithdrawalTopic.pipeInput(sinkEvent.getEvent().getSourceId(), sinkEvent);
        meterRegistry.getMeters().forEach(meter -> {
            if (meter instanceof Gauge gauge) {
                gauge.value();
            }
        });
        store =
                testDriver.getKeyValueStore("withdrawal-route-store");
        result = store.get(TEST_WITHDRAWAL_ID);
        assertNotNull(result);
        assertEquals(TEST_WITHDRAWAL_ID, result.getWithdrawalId());
        gaugeCaptured = getGaugeWithdrawalSucceeded();
        assertNull(gaugeCaptured);
        gaugePending = getGaugeWithdrawalPending();
        assertNull(gaugePending);

        MachineEvent statusChangedWithdrawalEvents = getStatusChangedWithdrawalEvents(TEST_WITHDRAWAL_ID);
        sinkEvent.setEvent(statusChangedWithdrawalEvents);
        inputWithdrawalTopic.pipeInput(sinkEvent.getEvent().getSourceId(), sinkEvent);
        meterRegistry.getMeters().forEach(meter -> {
            if (meter instanceof Gauge gauge) {
                gauge.value();
            }
        });
        store =
                testDriver.getKeyValueStore("withdrawal-status-store");
        result = store.get(TEST_WITHDRAWAL_ID);
        assertNotNull(result);
        assertEquals(TEST_WITHDRAWAL_ID, result.getWithdrawalId());
        gaugeCaptured = getGaugeWithdrawalSucceeded();
        assertNotNull(gaugeCaptured);
        assertEquals(1.0, gaugeCaptured.value());
        Collection<Meter> meters = getMeterWithdrawalSucceeded();
        assertEquals(1, meters.size());
    }

    private Gauge getGaugePaymentPending() {
        return meterRegistry
                .find(Metric.PAYMENTS_STATUS_COUNT.getName())
                .tags(
                        "shop_id", "test_shop_id",
                        "currency", "RUB",
                        "status", "pending",
                        "duration", "5m"
                )
                .gauge();
    }

    private Gauge getGaugePaymentPendingWithRoute() {
        return meterRegistry
                .find(Metric.PAYMENTS_STATUS_COUNT.getName())
                .tags(
                        "provider_id", "21",
                        "terminal_id", "35",
                        "shop_id", "test_shop_id",
                        "currency", "RUB",
                        "status", "pending",
                        "duration", "5m"
                )
                .gauge();
    }

    private Gauge getGaugePaymentCaptured() {
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

    private Collection<Meter> getMeterPaymentCaptured() {
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
                .meters();
    }

    private Gauge getGaugeWithdrawalPending() {
        return meterRegistry
                .find(Metric.WITHDRAWALS_STATUS_COUNT.getName())
                .tags(
                        "wallet_id", "test_wallet_id",
                        "currency", "RUB",
                        "status", "pending",
                        "duration", "5m"
                )
                .gauge();
    }

    private Gauge getGaugeWithdrawalSucceeded() {
        return meterRegistry
                .find(Metric.WITHDRAWALS_STATUS_COUNT.getName())
                .tags(
                        "provider_id", "1",
                        "terminal_id", "2",
                        "shop_id", "test_wallet_id",
                        "currency", "RUB",
                        "status", "succeeded",
                        "duration", "15m"
                )
                .gauge();
    }

    private Collection<Meter> getMeterWithdrawalSucceeded() {
        return meterRegistry
                .find(Metric.WITHDRAWALS_STATUS_COUNT.getName())
                .tags(
                        "provider_id", "1",
                        "terminal_id", "2",
                        "shop_id", "test_wallet_id",
                        "currency", "RUB",
                        "status", "succeeded",
                        "duration", "15m"
                )
                .meters();
    }

}
