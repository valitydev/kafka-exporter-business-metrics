package dev.vality.exporter.businessmetrics.listener;

import dev.vality.exporter.businessmetrics.config.KafkaPostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.dao.InvoicePaymentDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.utils.TestData;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@KafkaPostgresqlSpringBootITest
class InvoiceKafkaListenerTest {

    @Value("${kafka.topics.invoice.id}")
    private String topic;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Autowired
    private InvoicePaymentDao invoicePaymentDao;

    @Test
    void shouldProcessInvoiceEventFromKafka() throws Exception {
        String invoiceId = "invoice-kafka-1";

        MachineEvent machineEvent =
                TestData.getStartedInvoicePaymentEvents(invoiceId);

        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);

        testThriftKafkaProducer.send(topic, sinkEvent);

        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> {

                    InvoicePaymentData payment =
                            invoicePaymentDao.get(
                                    invoiceId,
                                    "1"
                            );

                    assertThat(payment).isNotNull();

                    assertThat(payment.getInvoiceId())
                            .isEqualTo(invoiceId);

                    assertThat(payment.getPaymentId())
                            .isEqualTo("1");

                    assertThat(payment.getAmount())
                            .isEqualTo(11L);

                    assertThat(payment.getCurrencyCode())
                            .isEqualTo("RUB");
                });
    }
}
