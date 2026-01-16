package dev.vality.exporter.businessmetrics.utils;

import dev.vality.damsel.base.Content;
import dev.vality.damsel.domain.Invoice;
import dev.vality.damsel.domain.InvoicePayment;
import dev.vality.damsel.domain.InvoicePaymentPending;
import dev.vality.damsel.domain.*;
import dev.vality.damsel.payment_processing.*;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.msgpack.Value;
import dev.vality.sink.common.serialization.impl.PaymentEventPayloadSerializer;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.List;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

public class TestData {

    public static final String TEST_SHOP_ID = "test_shop_id";
    public static final String TEST_PARTY_ID = "test_party_id";

    @SuppressWarnings("LineLength")
    public static MachineEvent getStartedInvoicePaymentEvents(String invoiceId) {
        PaymentEventPayloadSerializer paymentEventPayloadSerializer = new PaymentEventPayloadSerializer();
        return new MachineEvent()
                .setSourceNs("source_ns")
                .setSourceId(invoiceId)
                .setEventId(1)
                .setCreatedAt(temporalToString(LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)))
                .setData(Value.bin(paymentEventPayloadSerializer.serialize(
                        EventPayload.invoice_changes(
                                List.of(InvoiceChange.invoice_created(new InvoiceCreated()
                                                .setInvoice(buildInvoice(invoiceId))),
                                        InvoiceChange.invoice_status_changed(new InvoiceStatusChanged()
                                                .setStatus(
                                                        InvoiceStatus.fulfilled(
                                                                new InvoiceFulfilled("keks")))),
                                        InvoiceChange.invoice_payment_change(new InvoicePaymentChange()
                                                .setId("1")
                                                .setPayload(InvoicePaymentChangePayload
                                                        .invoice_payment_started(new InvoicePaymentStarted()
                                                                .setPayment(
                                                                        buildPayment("1"))
                                                                .setCashFlow(List.of(new FinalCashFlowPosting()
                                                                        .setSource(
                                                                                new FinalCashFlowAccount(
                                                                                        CashFlowAccount
                                                                                                .system(SystemCashFlowAccount.settlement),
                                                                                        1))
                                                                        .setDestination(
                                                                                new FinalCashFlowAccount(
                                                                                        CashFlowAccount
                                                                                                .system(SystemCashFlowAccount.settlement),
                                                                                        1))
                                                                        .setVolume(new Cash(2,
                                                                                new CurrencyRef(
                                                                                        "RUB")))))
                                                                .setRoute(new PaymentRoute(
                                                                        new ProviderRef(29),
                                                                        new TerminalRef(30)))
                                                        )))
                                )))));
    }

    public static MachineEvent getRouteChangedInvoicePaymentEvents(String invoiceId) {
        PaymentEventPayloadSerializer paymentEventPayloadSerializer = new PaymentEventPayloadSerializer();
        return new MachineEvent()
                .setSourceNs("source_ns")
                .setSourceId(invoiceId)
                .setEventId(2)
                .setCreatedAt(temporalToString(LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)))
                .setData(Value.bin(paymentEventPayloadSerializer.serialize(
                        EventPayload.invoice_changes(
                                List.of(InvoiceChange.invoice_created(new InvoiceCreated()
                                                .setInvoice(buildInvoice(invoiceId))),
                                        InvoiceChange.invoice_payment_change(new InvoicePaymentChange()
                                                .setId("1")
                                                .setPayload(InvoicePaymentChangePayload
                                                        .invoice_payment_route_changed(
                                                                new InvoicePaymentRouteChanged(new PaymentRoute(
                                                                        new ProviderRef(21),
                                                                        new TerminalRef(35))))))
                                )))));
    }

    public static MachineEvent getStatusChangedPaymentEvents(String invoiceId) {
        PaymentEventPayloadSerializer paymentEventPayloadSerializer = new PaymentEventPayloadSerializer();
        return new MachineEvent()
                .setSourceNs("source_ns")
                .setSourceId(invoiceId)
                .setEventId(3)
                .setCreatedAt(temporalToString(LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)))
                .setData(Value.bin(paymentEventPayloadSerializer.serialize(
                        EventPayload.invoice_changes(
                                List.of(
                                        InvoiceChange.invoice_payment_change(new InvoicePaymentChange()
                                                .setId("1")
                                                .setPayload(InvoicePaymentChangePayload
                                                        .invoice_payment_status_changed(
                                                                new InvoicePaymentStatusChanged().setStatus(
                                                                        InvoicePaymentStatus.captured(
                                                                                new InvoicePaymentCaptured()))
                                                        )))
                                )))));
    }

    public static Invoice buildInvoice(String invoiceId) {
        return new Invoice()
                .setId(invoiceId)
                .setPartyRef(new PartyConfigRef(TEST_PARTY_ID))
                .setShopRef(new ShopConfigRef(TEST_SHOP_ID))
                .setCreatedAt(temporalToString(LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)))
                .setStatus(InvoiceStatus.unpaid(new InvoiceUnpaid()))
                .setDetails(new InvoiceDetails()
                        .setProduct("prod")
                        .setCart(new InvoiceCart(
                                List.of(new InvoiceLine()
                                        .setQuantity(1)
                                        .setProduct("product")
                                        .setPrice(new Cash(12, new CurrencyRef("RUB")))
                                        .setMetadata(new HashMap<>())))))
                .setDue(temporalToString(LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)))
                .setCost(new Cash().setAmount(1).setCurrency(new CurrencyRef("RUB")))
                .setContext(new Content("type", ByteBuffer.wrap(new byte[]{})));
    }

    public static dev.vality.damsel.domain.InvoicePayment buildPayment(String paymentId) {
        return new InvoicePayment()
                .setShopRef(new ShopConfigRef(TEST_SHOP_ID))
                .setPartyRef(new PartyConfigRef(TEST_PARTY_ID))
                .setId(paymentId)
                .setCreatedAt(temporalToString(LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)))
                .setStatus(InvoicePaymentStatus.pending(new InvoicePaymentPending()))
                .setCost(new Cash(11, new CurrencyRef("RUB")))
                .setDomainRevision(1)
                .setFlow(InvoicePaymentFlow.instant(new InvoicePaymentFlowInstant()))
                .setPayer(Payer.recurrent(
                        new RecurrentPayer()
                                .setPaymentTool(PaymentTool.payment_terminal(
                                        new PaymentTerminal()
                                                .setPaymentService(new PaymentServiceRef("alipay"))))
                                .setRecurrentParent(new RecurrentParentPayment("1", "2"))
                                .setContactInfo(new ContactInfo())));
    }

    private static final DateTimeFormatter FORMATTER = ISO_INSTANT;

    public static String temporalToString(LocalDateTime localDateTime) throws IllegalArgumentException {
        return temporalToString(localDateTime.toInstant(ZoneOffset.UTC));
    }

    public static String temporalToString(TemporalAccessor temporalAccessor) throws IllegalArgumentException {
        try {
            return FORMATTER.format(temporalAccessor);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to format:" + temporalAccessor, e);
        }
    }
}
