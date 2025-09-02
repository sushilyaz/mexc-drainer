package com.suhoi.mexcdrainer.controller;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.Creds;
import com.suhoi.mexcdrainer.service.DrainService;
import com.suhoi.mexcdrainer.service.RangeDrainService;
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

    // ВАЖНО: final => Lombok заинжектит через конструктор
    private final RangeDrainService rangeDrainService;
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

            if (text.startsWith("/drain")) {
                String[] p = text.split("\\s+");
                // Проверим наличие ключей заранее
                var a = MemoryDb.getAccountA(chatId);
                var b = MemoryDb.getAccountB(chatId);
                if (a == null || b == null) {
                    tg.reply(chatId, "Сначала задайте ключи: /setA и /setB");
                    return;
                }

                // --- Режим 1: старый (/drain SYMBOL USDT)
                if (p.length == 3) {
                    final String symbol = p[1].toUpperCase();
                    final BigDecimal usdt = parseDecimalSafe(p[2]);
                    if (usdt == null) {
                        tg.reply(chatId, "Сумма USDT должна быть числом. Пример: /drain ANTUSDT 5");
                        return;
                    }
                    tg.reply(chatId, "▶️ Запускаю перелив: %s на %s USDT".formatted(symbol, usdt.stripTrailingZeros()));
                    // Синхронно, как у тебя и было (можно вынести в отдельный executor, если захочешь)
                    drainService.startDrain(symbol, usdt, chatId, 20);
                    return;
                }

                // --- Режим 2: новый диапазон (/drain SYMBOL LOW HIGH USDT)
                if (p.length == 5) {
                    final String symbol = p[1].toUpperCase();
                    final BigDecimal low = parseDecimalSafe(p[2]);
                    final BigDecimal high = parseDecimalSafe(p[3]);
                    final BigDecimal usdt = parseDecimalSafe(p[4]);

                    if (low == null || high == null || usdt == null) {
                        tg.reply(chatId, """
                                Некорректные числа.
                                Формат: /drain <SYMBOL> <LOW> <HIGH> <USDT>
                                Пример: /drain ANTUSDT 0,000010 0,000020 5
                                """);
                        return;
                    }
                    if (low.signum() <= 0 || high.signum() <= 0 || low.compareTo(high) >= 0) {
                        tg.reply(chatId, "Неверный диапазон цен: LOW должен быть > 0 и меньше HIGH.");
                        return;
                    }
                    if (usdt.signum() <= 0) {
                        tg.reply(chatId, "Сумма USDT должна быть > 0.");
                        return;
                    }

                    tg.reply(chatId, "▶️ Диапазонный перелив: %s в [%s .. %s], цель %s USDT"
                            .formatted(symbol,
                                    low.stripTrailingZeros().toPlainString(),
                                    high.stripTrailingZeros().toPlainString(),
                                    usdt.stripTrailingZeros().toPlainString()));

                    // В отдельном потоке, чтобы не блокировать Telegram LongPolling поток
                    new Thread(() -> {
                        try {
                            rangeDrainService.startDrainInRange(chatId, symbol, low, high, usdt);
                            tg.reply(chatId, "✅ Перелив по %s завершён.".formatted(symbol));
                        } catch (Exception e) {
                            log.error("Ошибка диапазонного перелива", e);
                            tg.reply(chatId, "❌ Ошибка перелива: " + e.getMessage());
                        }
                    }, "drain-range-" + symbol + "-" + chatId).start();

                    return;
                }

                // Если формат не распознан
                tg.reply(chatId, """
                        Неверный формат. Используйте:
                        /drain <SYMBOL> <USDT>  или
                        /drain <SYMBOL> <LOW> <HIGH> <USDT>
                        Примеры:
                        /drain ANTUSDT 5
                        /drain ANTUSDT 0,000010 0,000020 5
                        """);
                return;
            }

            if (text.equals("/stop")) {
                try {
                    rangeDrainService.stopRange(chatId);
                    tg.reply(chatId, "⏸️ Пауза включена. Текущий цикл остановится без принудительных продаж/отмен.");
                } catch (Exception e) {
                    tg.reply(chatId, "❌ Ошибка при остановке: " + e.getMessage());
                }
                return;
            }

            if (text.startsWith("/continue")) {
                String[] p = text.split("\\s+");
                if (p.length != 3) {
                    tg.reply(chatId, "Формат: /continue <LOW> <HIGH>\nПример: /continue 0,000057 0,0000585");
                    return;
                }
                BigDecimal low = parseDecimalSafe(p[1]);
                BigDecimal high = parseDecimalSafe(p[2]);
                if (low == null || high == null || low.compareTo(high) >= 0) {
                    tg.reply(chatId, "Неверные границы. Пример: /continue 0,000057 0,0000585");
                    return;
                }

                // Запустить "продолжение" в отдельном потоке, чтобы не блокировать телеграм-пуллинг
                new Thread(() -> {
                    try {
                        rangeDrainService.continueRange(chatId, low, high);
                        tg.reply(chatId, "▶️ Продолжаю в вилке [%s .. %s] (остаток цели из состояния)"
                                .formatted(low.stripTrailingZeros(), high.stripTrailingZeros()));
                    } catch (Exception e) {
                        tg.reply(chatId, "❌ Ошибка /continue: " + e.getMessage());
                    }
                }, "continue-range-" + chatId).start();

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


