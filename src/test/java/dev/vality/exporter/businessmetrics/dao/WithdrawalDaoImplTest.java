package dev.vality.exporter.businessmetrics.dao;

import dev.vality.exporter.businessmetrics.domain.enums.WithdrawalStatus;
import dev.vality.exporter.businessmetrics.domain.tables.pojos.WithdrawalData;
import dev.vality.exporter.businessmetrics.utils.TestData;
import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;

import static dev.vality.exporter.businessmetrics.domain.Tables.WITHDRAWAL_DATA;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@PostgresqlTestcontainer
@SpringBootTest
public class WithdrawalDaoImplTest {
    @Autowired
    private WithdrawalDao withdrawalDao;
    @Autowired
    private DSLContext dsl;

    @BeforeEach
    void clean() {
        dsl.deleteFrom(WITHDRAWAL_DATA).execute();
    }

    @Test
    void shouldSaveAndGetWithdrawal() throws Exception {
        LocalDateTime now = LocalDateTime.now();
        WithdrawalData expected = TestData.withdrawalData("withdrawal-1", now);
        Long id = withdrawalDao.save(expected);
        assertThat(id).isNotNull();
        WithdrawalData actual = withdrawalDao.get("withdrawal-1");
        assertThat(actual).isNotNull();
        assertThat(actual.getWithdrawalId()).isEqualTo("withdrawal-1");
        assertThat(actual.getAmount()).isEqualTo(200L);
        assertThat(actual.getCurrencyCode()).isEqualTo("RUB");
        assertThat(actual.getProviderId()).isEqualTo(21);
        assertThat(actual.getTerminalId()).isEqualTo(35);
    }

    @Test
    void shouldUpdateExistingWithdrawalOnConflict() throws Exception {
        WithdrawalData withdrawal = TestData.withdrawalData("withdrawal-1", LocalDateTime.now());
        withdrawalDao.save(withdrawal);
        withdrawal.setAmount(999L);
        withdrawal.setWithdrawalStatus(WithdrawalStatus.succeeded);
        withdrawalDao.save(withdrawal);
        WithdrawalData actual = withdrawalDao.get("withdrawal-1");
        assertThat(actual.getAmount()).isEqualTo(999L);
        assertThat(actual.getWithdrawalStatus()).isEqualTo(WithdrawalStatus.succeeded);
    }

    @Test
    void shouldReturnNullWhenWithdrawalDoesNotExist() throws Exception {
        WithdrawalData result = withdrawalDao.get("unknown");
        assertThat(result).isNull();
    }
}
