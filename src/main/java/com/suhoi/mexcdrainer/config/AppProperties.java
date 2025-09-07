package com.suhoi.mexcdrainer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "app")
public class AppProperties {
    private Telegram telegram;
    private Mexc mexc;
    private Drain drain;

    @Data
    public static class Telegram {
        private String botToken;
        private String botUsername = "drain_mexc_bot";
    }

    @Data
    public static class Mexc {
        private String baseUrl = "https://api.mexc.com";
        private long recvWindowMs = 5_000;
    }

    @Data
    public static class Drain {
        private int minSpreadTicks = 5;
        private int epsilonTicks = 1;
        private int maxRequotesPerLeg = 3;
        private int sleepBetweenRequotesMs = 120;
        private int depthLimit = 20;
        private String feeSafety = "0.0010";   // +0.10%
        private String priceGuardPct = "0.08"; // ±8% от lastPrice
        private int postPlaceGraceMs = 220;

        private Ws ws = new Ws(); // <-- НОВОЕ

        @Data
        public static class Ws {
            private boolean enabled = true;
            private int maxStalenessMs = 200;
            private int pingIntervalMs = 15_000;

            private Reconnect reconnect = new Reconnect();
            private Market market = new Market();
            private PrivateCfg privateCfg = new PrivateCfg();

            @Data
            public static class Reconnect {
                private int baseDelayMs = 200;
                private int maxDelayMs = 5_000;
                private int jitterPct = 30;
            }
            @Data
            public static class Market {
                private boolean useDepth = false;
                private String depthScale = "0.1";
                private int snapshotIntervalSec = 30;
            }
            @Data
            public static class PrivateCfg {
                private Integer refreshListenKeyMin = 25;
            }
        }
    }
}
