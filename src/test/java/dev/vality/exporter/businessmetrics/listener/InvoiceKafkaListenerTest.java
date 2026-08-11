package dev.vality.exporter.businessmetrics.listener;

import dev.vality.exporter.businessmetrics.config.KafkaPostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.config.KafkaTestProducerConfig;
import dev.vality.exporter.businessmetrics.dao.InvoicePaymentDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.InvoicePaymentData;
import dev.vality.exporter.businessmetrics.utils.TestData;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@KafkaPostgresqlSpringBootITest
@SpringBootTest(
        classes = {
                KafkaTestProducerConfig.class
        }
)
class InvoiceKafkaListenerTest {

    @Autowired
    private KafkaTemplate<String, SinkEvent> kafkaTemplate;

    @Autowired
    private InvoicePaymentDao invoicePaymentDao;

    @Test
    void shouldProcessInvoiceEventFromKafka() throws Exception {
        String invoiceId = "invoice-kafka-1";

        MachineEvent machineEvent =
                TestData.getStartedInvoicePaymentEvents(invoiceId);

        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);

        kafkaTemplate
                .send(
                        "invoice-test",
                        invoiceId,
                        sinkEvent
                )
                .get();

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
