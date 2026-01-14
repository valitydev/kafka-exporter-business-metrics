package dev.vality.exporter.businessmetrics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@ServletComponentScan
@SpringBootApplication
@EnableScheduling
public class ExporterBusinessMetricsApplication extends SpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExporterBusinessMetricsApplication.class, args);
    }

}
