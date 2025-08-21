package com.suhoi.mexcdrainer.telegram;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.service.TransferService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;

@Slf4j
@Component
@RequiredArgsConstructor
public class DrainBot extends TelegramLongPollingBot {
    private final AppProperties props;
    private final KeyStore keyStore;
    private final TransferService transferService;

    // Регистрация бота в API
    @EventListener(ApplicationReadyEvent.class)
    public void register() throws Exception {
        TelegramBotsApi api = new TelegramBotsApi(DefaultBotSession.class);
        api.registerBot(this);
        log.info("Telegram bot registered: {}", getBotUsername());
    }

    @Override public String getBotUsername() { return props.getTelegram().getBotUsername(); }
    @Override public String getBotToken()     { return props.getTelegram().getBotToken(); }

    @Override
    public void onUpdateReceived(Update update) {
        if (update == null || !update.hasMessage()) return;
        Message m = update.getMessage();
        if (!m.hasText()) return;

        Long chatId = m.getChatId();
        if (!allowed(chatId)) return;

        String text = m.getText().trim();
        String[] parts = text.split("\\s+");
        String cmd = normalizeCommand(parts[0]);

        try {
            switch (cmd) {
                case "/start", "/help" -> reply(chatId, """
                        Команды:
                        /setA <apiKey> <secret>
                        /setB <apiKey> <secret>
                        /drain_all <SYMBOL>   (пример: /drain_all ANTUSDT)
                        """);

                case "/seta" -> {
                    if (parts.length < 3) { reply(chatId, "Формат: /setA <apiKey> <secret>"); return; }
                    keyStore.setA(chatId, parts[1], parts[2]);
                    reply(chatId, "Ок. Аккаунт A сохранён.");
                }

                case "/setb" -> {
                    if (parts.length < 3) { reply(chatId, "Формат: /setB <apiKey> <secret>"); return; }
                    keyStore.setB(chatId, parts[1], parts[2]);
                    reply(chatId, "Ок. Аккаунт B сохранён.");
                }

                case "/drain_all" -> {
                    if (parts.length < 2) { reply(chatId, "Формат: /drain_all ANTUSDT"); return; }
                    var aKeys = keyStore.getA(chatId);
                    var bKeys = keyStore.getB(chatId);
                    if (aKeys == null || bKeys == null) { reply(chatId, "Сначала /setA и /setB"); return; }

                    String symbol = parts[1].toUpperCase();
                    reply(chatId, "Старт drain_all " + symbol + " …");

                    new Thread(() -> {
                        try {
                            String report = transferService.drainAll(chatId, symbol, aKeys, bKeys);
                            reply(chatId, "DONE\n" + report);
                        } catch (Exception e) {
                            log.error("Drain error", e);
                            reply(chatId, "Ошибка: " + e.getMessage());
                        }
                    }, "drain-" + symbol + "-" + chatId).start();
                }

                default -> reply(chatId, "Неизвестная команда. /help");
            }
        } catch (Exception e) {
            log.error("TG handling error", e);
            reply(chatId, "Ошибка: " + e.getMessage());
        }
    }

    private boolean allowed(Long chatId) {
        String allowed = props.getTelegram().getAllowedChatId();
        return allowed == null || allowed.equals(String.valueOf(chatId));
    }

    private String normalizeCommand(String raw) {
        // поддержка вида /cmd@BotName
        String c = raw.toLowerCase();
        int at = c.indexOf('@');
        return at >= 0 ? c.substring(0, at) : c;
    }

    private void reply(Long chatId, String text) {
        try {
            execute(SendMessage.builder().chatId(chatId.toString()).text(text).build());
        } catch (TelegramApiException e) {
            log.warn("TG send failed", e);
        }
    }
}
