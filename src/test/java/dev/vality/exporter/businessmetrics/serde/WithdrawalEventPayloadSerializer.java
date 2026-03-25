package dev.vality.exporter.businessmetrics.serde;

import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.sink.common.serialization.impl.ThriftBinarySerializer;

public class WithdrawalEventPayloadSerializer extends ThriftBinarySerializer<TimestampedChange> {

}
