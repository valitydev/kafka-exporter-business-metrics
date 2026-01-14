package dev.vality.exporter.businessmetrics.model;

import io.micrometer.core.instrument.Tag;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CustomTag {

    public static final String PROVIDER_ID_TAG = "provider_id";
    public static final String TERMINAL_ID_TAG = "terminal_id";
    public static final String SHOP_ID_TAG = "shop_id";
    public static final String CURRENCY_TAG = "currency";
    public static final String STATUS_TAG = "status";
    public static final String WALLET_ID_TAG = "wallet_id";
    public static final String DURATION_TAG = "duration";

    public static Tag providerId(String providerId) {
        return Tag.of(PROVIDER_ID_TAG, providerId);
    }

    public static Tag terminalId(String terminalId) {
        return Tag.of(TERMINAL_ID_TAG, terminalId);
    }


    public static Tag shopId(String shopId) {
        return Tag.of(SHOP_ID_TAG, shopId);
    }


    public static Tag currency(String currency) {
        return Tag.of(CURRENCY_TAG, currency);
    }

    public static Tag status(String status) {
        return Tag.of(STATUS_TAG, status);
    }

    public static Tag walletId(String walletId) {
        return Tag.of(WALLET_ID_TAG, walletId);
    }

    public static Tag duration(String duration) {
        return Tag.of(DURATION_TAG, duration);
    }

}
