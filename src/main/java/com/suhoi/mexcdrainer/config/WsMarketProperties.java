package com.suhoi.mexcdrainer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "mexc.ws")
public class WsMarketProperties {
    /**
     * Базовая точка входа Market WS. Пример: wss://wbs-api.mexc.com/ws
     */
    private String endpoint = "wss://wbs-api.mexc.com/ws";

    /** Период PING (сек). */
    private int pingPeriodSec = 25;

    /** Принудительная ротация соединения, часов (не больше 24 на одном коннекте). */
    private int rotateAfterHours = 23;

    /** Базовая задержка для экспоненциального ре-коннекта (сек). */
    private int reconnectBackoffBaseSec = 2;

    /** Максимальная задержка ре-коннекта (сек). */
    private int reconnectBackoffMaxSec = 60;

    /** Максимум подписок на одно WS-соединение. */
    private int maxSubscriptionsPerConn = 30;

    /** Период каналов bookTicker/depth в миллисекундах: 10 или 100. */
    private int intervalMs = 100;

    /** Размер partial-среза книги (5/10/20 уровней). */
    private int partialLevels = 10;

    /** Таймаут «черствости» книги, мс — если нет апдейтов дольше, считаем STALE. */
    private long staleMs = 300;

    /** Бэк-офф перед повторной инициализацией частичного снапшота, мс. */
    private long resyncBackoffMs = 200;
}
