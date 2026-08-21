package dev.vality.exporter.businessmetrics.service;

import dev.vality.exporter.businessmetrics.config.properties.CleanupProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static dev.vality.exporter.businessmetrics.domain.Tables.INVOICE_PAYMENT_DATA;
import static dev.vality.exporter.businessmetrics.domain.Tables.WITHDRAWAL_DATA;

@Slf4j
@Service
@RequiredArgsConstructor
public class DataCleanupService {

    private final DSLContext dsl;
    private final CleanupProperties cleanupProperties;

    @Scheduled(cron = "${exporter.cleanup.cron}")
    public void cleanup() {
        LocalDateTime threshold = LocalDateTime.now().minusDays(cleanupProperties.getRetentionDays());
        log.info("Start dataBase cleanup. threshold={}", threshold);
        int paymentsDeleted = deleteOldPayments(threshold);
        int withdrawalsDeleted = deleteOldWithdrawals(threshold);
        log.info(
                "DataBase cleanup finished. paymentsDeleted={}, withdrawalsDeleted={}",
                paymentsDeleted, withdrawalsDeleted);
    }

    private int deleteOldPayments(
            LocalDateTime threshold
    ) {
        int total = 0;
        while (true) {
            int deleted =
                    dsl.deleteFrom(INVOICE_PAYMENT_DATA)
                            .where(
                                    INVOICE_PAYMENT_DATA.ID.in(
                                            dsl.select(
                                                            INVOICE_PAYMENT_DATA.ID
                                                    )
                                                    .from(INVOICE_PAYMENT_DATA)
                                                    .where(
                                                            INVOICE_PAYMENT_DATA.CREATED_AT
                                                                    .lt(threshold)
                                                    )
                                                    .limit(cleanupProperties.getBatchSize())
                                    )
                            )
                            .execute();

            log.debug("DataBase cleanup progress payments. deletedInBatch={}, totalDeleted={}, threshold={}",
                    deleted, total, threshold);
            total += deleted;
            if (deleted < cleanupProperties.getBatchSize()) {
                break;
            }
        }
        return total;
    }

    private int deleteOldWithdrawals(
            LocalDateTime threshold
    ) {
        int total = 0;
        while (true) {
            int deleted =
                    dsl.deleteFrom(WITHDRAWAL_DATA)
                            .where(
                                    WITHDRAWAL_DATA.ID.in(
                                            dsl.select(
                                                            WITHDRAWAL_DATA.ID
                                                    )
                                                    .from(WITHDRAWAL_DATA)
                                                    .where(
                                                            WITHDRAWAL_DATA.CREATED_AT
                                                                    .lt(threshold)
                                                    )
                                                    .limit(cleanupProperties.getBatchSize())
                                    )
                            )
                            .execute();
            log.debug("DataBase cleanup progress withdrawals. deletedInBatch={}, totalDeleted={}, threshold={}",
                    deleted, total, threshold);
            total += deleted;
            if (deleted < cleanupProperties.getBatchSize()) {
                break;
            }
        }
        return total;
    }
}
