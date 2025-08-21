package com.suhoi.mexcdrainer.config;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@ConfigurationProperties(prefix = "app")
@Data
public class AppProperties {
    private Telegram telegram = new Telegram();
    private Mexc mexc = new Mexc();

    @Data
    public static class Telegram {
        private String botToken;
        private String botUsername;
        private String allowedChatId; // optional
    }

    @Data
    public static class Mexc {
        @NotBlank
        private String baseUrl = "https://api.mexc.com";
        private long recvWindowMs = 5000;
        private long pollMs = 600;
        private int maxReposts = 100;
        private int maxCycles = 100;
        private double takerFeePct = 0.0005;
        private double safetySpendPct = 0.998;
    }
}
