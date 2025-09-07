package com.suhoi.mexcdrainer.controller;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.Creds;
import com.suhoi.mexcdrainer.service.DrainService;
import com.suhoi.mexcdrainer.service.TelegramService;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.objects.Update;

import java.math.BigDecimal;

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
                        Привет! Я бот для перелива через спред на MEXC.

                        Команды:
                        /setA <apiKey> <secretKey> — задать ключи Аккаунта A (С КОТОРОГО переливаем)
                        /setB <apiKey> <secretKey> — задать ключи Аккаунта B (НА КОТОРЫЙ переливаем)

                        Режимы перелива:
                        1) Простой:   /drain <SYMBOL> <USDT>
                           пример: /drain ANTUSDT 5

                        2) В диапазоне: /drain <SYMBOL> <LOW> <HIGH> <USDT>
                           пример: /drain ANTUSDT 0,000010 0,000020 5
                           Цены можно писать с запятой или точкой.

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

            if (text.startsWith("/status")) {
                String s = drainService.status(chatId);
                tg.reply(chatId, s);
                return;
            }

            if (text.startsWith("/stop")) {
                drainService.requestStop(chatId);
                tg.reply(chatId, "⏸ Поставил на паузу (MANUAL).");
                return;
            }

            if (text.startsWith("/continue")) {
                String[] p = text.split("\\s+");
                if (p.length < 2) {
                    tg.reply(chatId, "Формат: /continue <SYMBOL> [cycles]\nпример: /continue ANTUSDT 20");
                    return;
                }
                String symbol = p[1].toUpperCase();
                int cycles = (p.length >= 3) ? Integer.parseInt(p[2]) : 20;
                tg.reply(chatId, "▶️ Продолжаю из фактических балансов по %s".formatted(symbol));
                drainService.continueFromBalances(symbol, chatId, cycles);
                return;
            }

            if (text.startsWith("/drain")) {
                // как у тебя, плюс проверка ключей ...
                String[] p = text.split("\\s+");
                var a = MemoryDb.getAccountA(chatId);
                var b = MemoryDb.getAccountB(chatId);
                if (a == null || b == null) {
                    tg.reply(chatId, "Сначала задайте ключи: /setA и /setB");
                    return;
                }
                if (p.length == 3) {
                    final String symbol = p[1].toUpperCase();
                    final BigDecimal usdt = parseDecimalSafe(p[2]);
                    if (usdt == null) {
                        tg.reply(chatId, "Сумма USDT должна быть числом. Пример: /drain ANTUSDT 5");
                        return;
                    }
                    tg.reply(chatId, "▶️ Запускаю перелив: %s на %s USDT".formatted(symbol, usdt.stripTrailingZeros()));
                    drainService.startDrain(symbol, usdt, chatId, 20);
                    return;
                }
                tg.reply(chatId, "Неверный формат. Пример: /drain ANTUSDT 5");
                return;
            }

            tg.reply(chatId, "Неизвестная команда. Наберите /start");

        } catch (Exception ex) {
            log.error("Ошибка обработки апдейта", ex);
            tg.reply(chatId, "❌ Ошибка: " + ex.getMessage());
        }
    }

    // --- Utils ---

    /**
     * Безопасный парсер десятичных чисел: поддерживает запятую и точку.
     * Возвращает null при ошибке.
     */
    private static BigDecimal parseDecimalSafe(String s) {
        if (s == null) return null;
        try {
            return new BigDecimal(s.trim().replace(',', '.'));
        } catch (Exception e) {
            return null;
        }
    }
}


