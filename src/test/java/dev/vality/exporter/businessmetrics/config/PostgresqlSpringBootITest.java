package dev.vality.exporter.businessmetrics.config;

import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest
@PostgresqlTestcontainerSingleton
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:postgresql://localhost:5432/exporter_metrics",
        "spring.datasource.username=postgres",
        "spring.datasource.password=postgres"
})
public @interface PostgresqlSpringBootITest {
}
