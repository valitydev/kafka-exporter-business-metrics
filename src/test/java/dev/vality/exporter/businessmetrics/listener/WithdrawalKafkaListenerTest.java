package dev.vality.exporter.businessmetrics.listener;

import dev.vality.exporter.businessmetrics.config.KafkaPostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.config.KafkaTestProducerConfig;
import dev.vality.exporter.businessmetrics.dao.WithdrawalDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
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
class WithdrawalKafkaListenerTest {

    @Autowired
    private KafkaTemplate<String, SinkEvent> kafkaTemplate;

    @Autowired
    private WithdrawalDao withdrawalDao;

    @Test
    void shouldProcessInvoiceEventFromKafka() throws Exception {
        String withdrawalId = "withdrawal-kafka-1";

        MachineEvent machineEvent =
                TestData.getStartedWithdrawalEvents(withdrawalId);

        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);

        kafkaTemplate
                .send(
                        "withdrawal-test",
                        withdrawalId,
                        sinkEvent
                )
                .get();

        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> {

                    WithdrawalData withdrawal = withdrawalDao.get(withdrawalId);

                    assertThat(withdrawal).isNotNull();

                    assertThat(withdrawal.getWithdrawalId())
                            .isEqualTo(withdrawalId);

                    assertThat(withdrawal.getAmount())
                            .isEqualTo(11L);

                    assertThat(withdrawal.getCurrencyCode())
                            .isEqualTo("RUB");
                });
    }
}
