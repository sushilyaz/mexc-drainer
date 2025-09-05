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
        private String botToken="8013267846:AAFRYVCj2Et9cRZZmKdxjq7JhnzG8O82mck";
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
        private String feeSafety = "0.0010"; // строкой, чтобы биндинг BigDecimal был надёжным
        private String priceGuardPct = "0.08"; // ±8% коридор от lastPrice
        private int postPlaceGraceMs = 220;
    }
}
