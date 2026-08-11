package dev.vality.exporter.businessmetrics.config;

import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@PostgresqlTestcontainerSingleton
@EmbeddedKafka(partitions = 1, topics = {
        "invoice-test",
        "withdrawal-test"
})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=kafka-test",
        "kafka.topics.invoice.id=invoice-test",
        "kafka.topics.invoice.enabled=true",
        "kafka.topics.withdrawal.id=withdrawal-test",
        "kafka.topics.withdrawal.enabled=true",
        "kafka.state.cache.size=0"
})
@PostgresqlSpringBootITest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public @interface KafkaPostgresqlSpringBootITest {
}
