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
}
