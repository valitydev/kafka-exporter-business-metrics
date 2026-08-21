package dev.vality.exporter.businessmetrics.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetricValues {

    private Long count;
    private Long amount;
}
