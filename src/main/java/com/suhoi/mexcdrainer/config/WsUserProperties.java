package com.suhoi.mexcdrainer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Общие настройки user-stream (listenKey keepalive и т.п.).
 * ВАЖНО: apiKey/secret берём из MemoryDb (на чат/аккаунт),
 * здесь — только общая политика.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "mexc.user")
public class WsUserProperties {
    /** Раз в сколько минут слать keepalive listenKey. */
    private int keepAliveMinutes = 30;

    /** Сколько ждать FILLED по WS до фолбэка в REST, мс. */
    private long awaitFilledTimeoutMs = 6_000;

    /** TTL «свежести» баланса из приватного WS, мс, после чего можно дернуть REST. */
    private long balanceFreshTtlMs = 2_000;
}
