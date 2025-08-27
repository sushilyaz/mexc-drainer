package com.suhoi.mexcdrainer.controller;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.Creds;
import com.suhoi.mexcdrainer.service.DrainService;
import com.suhoi.mexcdrainer.service.TelegramService;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.objects.Update;

import java.math.BigDecimal;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
@RequiredArgsConstructor
public class TelegramBotHandler extends TelegramLongPollingBot {

    private final DrainService drainService;
    private final AppProperties appProperties;
    private final TelegramService tg;

    @Override
    public String getBotUsername() {
        return appProperties.getTelegram().getBotUsername();
    }

    @Override
    public String getBotToken() {
        return appProperties.getTelegram().getBotToken();
    }

    @Override
    public void onUpdateReceived(Update update) {
        if (update.getMessage() == null || update.getMessage().getText() == null) return;

        final long chatId = update.getMessage().getChatId();
        final String text = update.getMessage().getText().trim();

        try {
            if (text.startsWith("/start")) {
                tg.reply(chatId, """
                        Привет! Я бот для перелива USDT через спред на MEXC.

                        Команды:
                        /setA <apiKey> <secretKey> — задать ключи Аккаунта A (С КОТОРОГО переливаем)
                        /setB <apiKey> <secretKey> — задать ключи Аккаунта B (НА КОТОРЫЙ переливаем)
                        /drain <SYMBOL> <USDT>    — запустить перелив (пример: /drain ANTUSDT 5)

                        ⚠️ Ключи хранятся только в памяти процесса и пропадут при перезапуске.
                        """);
                return;
            }

            if (text.startsWith("/setA")) {
                String[] p = text.split("\\s+");
                if (p.length != 3) {
                    tg.reply(chatId, "Формат: /setA <apiKey> <secretKey>");
                    return;
                }
                MemoryDb.setAccountA(chatId, new Creds(p[1], p[2]));
                tg.reply(chatId, "✅ Ключи Аккаунта A сохранены (in-memory).");
                return;
            }

            if (text.startsWith("/setB")) {
                String[] p = text.split("\\s+");
                if (p.length != 3) {
                    tg.reply(chatId, "Формат: /setB <apiKey> <secretKey>");
                    return;
                }
                MemoryDb.setAccountB(chatId, new Creds(p[1], p[2]));
                tg.reply(chatId, "✅ Ключи Аккаунта B сохранены (in-memory).");
                return;
            }

            if (text.startsWith("/drain")) {
                String[] p = text.split("\\s+");
                if (p.length != 3) {
                    tg.reply(chatId, "Формат: /drain <SYMBOL> <USDT>\nНапример: /drain ANTUSDT 5");
                    return;
                }
                final String symbol = p[1].toUpperCase();
                final BigDecimal usdt;
                double usdtDouble = Double.parseDouble(p[2]);
                try {
                    usdt = BigDecimal.valueOf(usdtDouble);
                } catch (NumberFormatException nfe) {
                    tg.reply(chatId, "Сумма USDT должна быть числом. Пример: /drain ANTUSDT 5");
                    return;
                }

                var a = MemoryDb.getAccountA(chatId);
                var b = MemoryDb.getAccountB(chatId);
                if (a == null || b == null) {
                    tg.reply(chatId, "Сначала задайте ключи: /setA и /setB");
                    return;
                }

                tg.reply(chatId, "▶️ Запускаю перелив: %s на %.4f USDT".formatted(symbol, usdt));
                // Выполняем синхронно (при желании можно вынести в отдельный executor)
                drainService.startDrain(symbol, usdt, chatId, 5);
                return;
            }

            tg.reply(chatId, "Неизвестная команда. Наберите /start");

        } catch (Exception ex) {
            log.error("Ошибка обработки апдейта", ex);
            tg.reply(chatId, "❌ Ошибка: " + ex.getMessage());
        }
    }
}

