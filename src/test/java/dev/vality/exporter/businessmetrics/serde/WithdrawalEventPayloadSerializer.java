package dev.vality.exporter.businessmetrics.serde;

import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.sink.common.serialization.impl.ThriftBinarySerializer;

public class WithdrawalEventPayloadSerializer extends ThriftBinarySerializer<TimestampedChange> {

}
