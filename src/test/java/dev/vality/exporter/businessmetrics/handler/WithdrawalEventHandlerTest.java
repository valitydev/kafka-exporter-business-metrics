package dev.vality.exporter.businessmetrics.handler;

import dev.vality.dao.DaoException;
import dev.vality.exporter.businessmetrics.config.PostgresqlSpringBootITest;
import dev.vality.exporter.businessmetrics.dao.WithdrawalDao;
import dev.vality.exporter.businessmetrics.domain.enums.WithdrawalStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
import dev.vality.exporter.businessmetrics.exception.StorageException;
import dev.vality.exporter.businessmetrics.handler.withdrawal.WithdrawalCreatedEventHandler;
import dev.vality.exporter.businessmetrics.handler.withdrawal.WithdrawalRouteChangedEventHandler;
import dev.vality.exporter.businessmetrics.handler.withdrawal.WithdrawalStatusChangedEventHandler;
import dev.vality.exporter.businessmetrics.kafka.serde.WithdrawalChangeDeserializer;
import dev.vality.exporter.businessmetrics.utils.TestData;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@PostgresqlSpringBootITest
class WithdrawalEventHandlerTest {

    @MockitoBean
    private WithdrawalDao withdrawalDao;

    @Autowired
    private WithdrawalCreatedEventHandler withdrawalCreatedEventHandler;

    @Autowired
    private WithdrawalRouteChangedEventHandler withdrawalRouteChangedEventHandler;

    @Autowired
    private WithdrawalStatusChangedEventHandler withdrawalStatusChangedEventHandler;


    @Test
    void shouldSaveWithdrawalData() throws Exception {
        String withdrawalId = "withdrawal-1";

        MachineEvent event =
                TestData.getStartedWithdrawalEvents(withdrawalId);

        TimestampedChange change = extractWithdrawalChange(event);

        when(withdrawalDao.save(any(WithdrawalData.class)))
                .thenReturn(1L);

        withdrawalCreatedEventHandler.handle(change, event);

        ArgumentCaptor<WithdrawalData> captor =
                ArgumentCaptor.forClass(WithdrawalData.class);

        verify(withdrawalDao).save(captor.capture());

        WithdrawalData actual = captor.getValue();

        assertThat(actual.getWithdrawalId())
                .isEqualTo(withdrawalId);

        assertThat(actual.getWalletId())
                .isEqualTo(TestData.TEST_WALLET_ID);

        assertThat(actual.getPartyId())
                .isEqualTo(UUID.fromString(TestData.TEST_PARTY_ID));

        assertThat(actual.getCurrencyCode())
                .isEqualTo("RUB");

        assertThat(actual.getAmount())
                .isEqualTo(11L);

        assertThat(actual.getWithdrawalStatus())
                .isEqualTo(WithdrawalStatus.pending);
    }


    @Test
    void shouldThrowStorageExceptionWhenDaoFailsOnCreate()
            throws Exception {

        String withdrawalId = "withdrawal-1";

        MachineEvent event =
                TestData.getStartedWithdrawalEvents(withdrawalId);

        TimestampedChange change = extractWithdrawalChange(event);

        when(withdrawalDao.save(any(WithdrawalData.class)))
                .thenThrow(new DaoException("DB error"));

        assertThatThrownBy(() ->
                withdrawalCreatedEventHandler.handle(change, event)
        )
                .isInstanceOf(StorageException.class);

        verify(withdrawalDao).save(any(WithdrawalData.class));
    }


    @Test
    void shouldUpdateProviderAndTerminal()
            throws Exception {

        String withdrawalId = "withdrawal-1";

        WithdrawalData existing =
                TestData.withdrawalData(
                        withdrawalId,
                        LocalDateTime.now()
                );

        when(withdrawalDao.get(withdrawalId))
                .thenReturn(existing);

        when(withdrawalDao.save(any(WithdrawalData.class)))
                .thenReturn(1L);

        MachineEvent event =
                TestData.getRouteChangedWithdrawalEvents(withdrawalId);

        TimestampedChange change = extractWithdrawalChange(event);

        withdrawalRouteChangedEventHandler.handle(change, event);

        ArgumentCaptor<WithdrawalData> captor =
                ArgumentCaptor.forClass(WithdrawalData.class);

        verify(withdrawalDao).save(captor.capture());

        WithdrawalData actual = captor.getValue();

        assertThat(actual.getWithdrawalId())
                .isEqualTo(withdrawalId);

        assertThat(actual.getProviderId())
                .isEqualTo(1);

        assertThat(actual.getTerminalId())
                .isEqualTo(2);
    }


    @Test
    void shouldThrowNotFoundWhenWithdrawalDoesNotExistForRouteChange()
            throws Exception {

        String withdrawalId = "withdrawal-1";

        when(withdrawalDao.get(withdrawalId))
                .thenReturn(null);

        MachineEvent event =
                TestData.getRouteChangedWithdrawalEvents(withdrawalId);

        TimestampedChange change = extractWithdrawalChange(event);

        assertThatCode(() ->
                withdrawalRouteChangedEventHandler.handle(change, event)
        ).doesNotThrowAnyException();

        verify(withdrawalDao)
                .get(withdrawalId);

        verify(withdrawalDao, never())
                .save(any());
    }


    @Test
    void shouldThrowStorageExceptionWhenDaoFailsOnGetForRouteChange()
            throws Exception {

        String withdrawalId = "withdrawal-1";

        when(withdrawalDao.get(withdrawalId))
                .thenThrow(new DaoException("DB error"));

        MachineEvent event =
                TestData.getRouteChangedWithdrawalEvents(withdrawalId);

        TimestampedChange change = extractWithdrawalChange(event);

        assertThatThrownBy(() ->
                withdrawalRouteChangedEventHandler.handle(change, event)
        )
                .isInstanceOf(StorageException.class);

        verify(withdrawalDao)
                .get(withdrawalId);

        verify(withdrawalDao, never())
                .save(any());
    }


    @Test
    void shouldUpdateWithdrawalStatus()
            throws Exception {

        String withdrawalId = "withdrawal-1";

        WithdrawalData existing =
                TestData.withdrawalData(
                        withdrawalId,
                        LocalDateTime.now()
                );

        existing.setWithdrawalStatus(WithdrawalStatus.pending);

        when(withdrawalDao.get(withdrawalId))
                .thenReturn(existing);

        when(withdrawalDao.save(any(WithdrawalData.class)))
                .thenReturn(1L);

        MachineEvent event =
                TestData.getStatusChangedWithdrawalEvents(withdrawalId);

        TimestampedChange change = extractWithdrawalChange(event);

        withdrawalStatusChangedEventHandler.handle(change, event);

        ArgumentCaptor<WithdrawalData> captor =
                ArgumentCaptor.forClass(WithdrawalData.class);

        verify(withdrawalDao).save(captor.capture());

        WithdrawalData actual = captor.getValue();

        assertThat(actual.getWithdrawalId())
                .isEqualTo(withdrawalId);

        assertThat(actual.getWithdrawalStatus())
                .isEqualTo(WithdrawalStatus.succeeded);
    }


    @Test
    void shouldThrowNotFoundWhenWithdrawalDoesNotExistForStatusChange()
            throws Exception {

        String withdrawalId = "withdrawal-1";

        when(withdrawalDao.get(withdrawalId))
                .thenReturn(null);

        MachineEvent event =
                TestData.getStatusChangedWithdrawalEvents(withdrawalId);

        TimestampedChange change = extractWithdrawalChange(event);

        assertThatCode(() ->
                withdrawalStatusChangedEventHandler.handle(change, event)
        ).doesNotThrowAnyException();

        verify(withdrawalDao)
                .get(withdrawalId);

        verify(withdrawalDao, never())
                .save(any());
    }


    @Test
    void shouldThrowStorageExceptionWhenDaoFailsOnGetForStatusChange()
            throws Exception {

        String withdrawalId = "withdrawal-1";

        when(withdrawalDao.get(withdrawalId))
                .thenThrow(new DaoException("DB error"));

        MachineEvent event =
                TestData.getStatusChangedWithdrawalEvents(withdrawalId);

        TimestampedChange change = extractWithdrawalChange(event);

        assertThatThrownBy(() ->
                withdrawalStatusChangedEventHandler.handle(change, event)
        )
                .isInstanceOf(StorageException.class);

        verify(withdrawalDao)
                .get(withdrawalId);

        verify(withdrawalDao, never())
                .save(any());
    }


    private TimestampedChange extractWithdrawalChange(
            MachineEvent event
    ) {
        WithdrawalChangeDeserializer payloadDeserializer =
                new WithdrawalChangeDeserializer();

        return payloadDeserializer.deserialize(event.data.getBin());
    }
}
