package com.suhoi.mexcdrainer.config;

import com.suhoi.mexcdrainer.controller.TelegramBotHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;

@Slf4j
@Configuration
public class TelegramConfig {

    @Bean
    public TelegramBotsApi telegramBotsApi() throws Exception {
        return new TelegramBotsApi(DefaultBotSession.class);
    }

    @Bean
    public CommandLineRunner registerDrainBot(TelegramBotsApi api, TelegramBotHandler handler) {
        return args -> {
            api.registerBot(handler);
            log.info("Telegram bot registered: {}", handler.getBotUsername());
        };
    }
}
