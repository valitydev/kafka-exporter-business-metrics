package dev.vality.exporter.businessmetrics.listener;

import dev.vality.exporter.businessmetrics.config.KafkaPostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.dao.WithdrawalDao;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
import dev.vality.exporter.businessmetrics.utils.TestData;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@KafkaPostgresqlSpringBootITest
class WithdrawalKafkaListenerTest {

    @Value("${kafka.topics.withdrawal.id}")
    private String topic;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Autowired
    private WithdrawalDao withdrawalDao;

    @Test
    void shouldProcessInvoiceEventFromKafka() throws Exception {
        String withdrawalId = "withdrawal-kafka-1";

        MachineEvent machineEvent =
                TestData.getStartedWithdrawalEvents(withdrawalId);

        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);

        testThriftKafkaProducer.send(topic, sinkEvent);

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
