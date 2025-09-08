package com.suhoi.mexcdrainer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Конфигурация приложения (prefix = "app").
 * Структура соответствует application.yml и даёт удобные шорткаты для доступа к WS-настройкам.
 *
 * Важные моменты:
 * - Все проценты/денежные коэффициенты оставлены строками (String), чтобы избежать проблем биндинга BigDecimal.
 * - Mexc.Ws содержит как userUrl из YAML, так и удобный getPrivateUrl(), который вернёт userUrl,
 *   если privateUrl не задан (совместимо с кодом, который ожидает "privateUrl").
 * - В корневом классе есть getWs(), который пробрасывает mexc.ws — это сохраняет совместимость
 *   с участками кода, где делали props.getWs().
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private Telegram telegram = new Telegram();
    private Mexc mexc = new Mexc();
    private Drain drain = new Drain();

    /**
     * Удобный шорткат для доступа к app.mexc.ws
     * (сохраняет совместимость с кодом, где вызывали props.getWs()).
     */
    public Mexc.Ws getWs() {
        return (mexc != null) ? mexc.getWs() : null;
    }

    // ==========================
    // Разделы конфигурации
    // ==========================

    @Data
    public static class Telegram {
        /** Токен бота Telegram. */
        private String botToken;
        /** Username бота (без @). */
        private String botUsername;
    }

    @Data
    public static class Mexc {
        /** REST-база для MEXC Spot API. */
        private String baseUrl = "https://api.mexc.com";
        /** recvWindow для подписанных запросов. */
        private long recvWindowMs = 5_000;

        // дополнительные параметры из YAML (оставлены на будущее и метрики)
        /** Интервал REST-пула (если где-то останется как фолбэк). */
        private Integer pollMs;
        /** Лимит переотправок/ретраев в старых блоках (если используется). */
        private Integer maxReposts;
        /** Глобальный лимит циклов перелива (если используется в CLI/режиме теста). */
        private Integer maxCycles;
        /** Такер-комиссия, доля (например "0.0005"). */
        private String takerFeePct;
        /** Защитный коэффициент траты (например "0.998"). */
        private String safetySpendPct;

        /** Настройки WebSocket. */
        private Ws ws = new Ws();

        @Data
        public static class Ws {
            /** Публичный WS для рыночных данных. */
            private String publicUrl = "wss://wbs.mexc.com/ws";
            /**
             * URL приватного WS для listenKey. В YAML используется ключ userUrl.
             * Если privateUrl в YAML не задан, используем userUrl.
             */
            private String privateUrl; // опционально
            /** Альтернативное имя из YAML (будет забинжено сюда). */
            private String userUrl;    // как в YAML

            /** Интервал bookTicker (например "10ms" или "100ms"). */
            private String bookTickerInterval = "10ms";
            /** Интервал глубины (snapshot/increments). */
            private String depthInterval = "10ms";
            /** Глубина snapshot при ресинке книги (если используешь REST). */
            private int depthLevels = 1000;

            /** Интервал "логических" пингов (на уровне сервиса). */
            private int pingIntervalMs = 15_000;
            /** Интервал отправки WS Ping frame. */
            private int wsPingFrameIntervalMs = 20_000;

            private Reconnect reconnect = new Reconnect();
            private ListenKey listenKey = new ListenKey();

            /**
             * Удобный геттер для кода, который ждёт "privateUrl".
             * Если privateUrl пуст, вернёт userUrl.
             */
            public String getPrivateUrl() {
                if (StringUtils.hasText(privateUrl)) return privateUrl;
                return userUrl;
            }

            /**
             * Удобный геттер количества минут для keep-alive listenKey.
             * Берёт значение из блока listenKey (по умолчанию 30).
             */
            public int getListenKeyKeepAliveMinutes() {
                return (listenKey != null) ? listenKey.getKeepAliveEveryMin() : 30;
            }

            @Data
            public static class Reconnect {
                /** Базовая задержка (мс) при reconnect с экспоненциальной лестницей. */
                private int baseDelayMs = 500;
                /** Максимальная задержка (мс) между попытками reconnect. */
                private int maxDelayMs = 10_000;
            }

            @Data
            public static class ListenKey {
                /** Как часто продлевать listenKey, в минутах. */
                private int keepAliveEveryMin = 30;
            }
        }
    }

    @Data
    public static class Drain {
        /** Минимальный спред (в тиках), при котором имеет смысл крутить цикл. */
        private int minSpreadTicks = 5;
        /** Чувствительность перестановки (в тиках) — на сколько "подрезать" для топа. */
        private int epsilonTicks = 1;
        /** Максимум перестановок в одной ноге. */
        private int maxRequotesPerLeg = 3;
        /** (Наследие) Пауза между перестановками — желательно заменить на событийную логику. */
        private int sleepBetweenRequotesMs = 120;
        /** Ограничение глубины при анализе книги. */
        private int depthLimit = 20;

        /** Запас по комиссиям/ошибкам округления (строкой для безопасного биндинга). */
        private String feeSafety = "0.0010";
        /** Коридор защитного гварда цены от lastPrice (строкой, доля; напр., "0.08"). */
        private String priceGuardPct = "0.08";

        /** Короткий grace после размещения, мс (в «быстром» сценарии ограничиваем ещё сильнее). */
        private int postPlaceGraceMs = 220;
    }
}
