package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;

/**
 * Отправляет сообщения в Telegram БЕЗ зависимости от DrainBot,
 * чтобы не создавать цикл бинов. Используем прямой вызов Telegram Bot API.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TelegramService {

    private final AppProperties props;
    private final RestClient http = RestClient.builder()
            .baseUrl("https://api.telegram.org")
            .build();

    public void reply(long chatId, String text) {
        String token = props.getTelegram().getBotToken();
        if (token == null || token.isBlank()) {
            log.warn("Telegram TOKEN не задан. Сообщение (chatId={}): {}", chatId, text);
            return;
        }

        try {
            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("chat_id", Long.toString(chatId));
            form.add("text", text);
            form.add("disable_web_page_preview", "true");
            // Если хочешь HTML/Markdown — раскомментируй:
            // form.add("parse_mode", "HTML");

            http.post()
                    .uri("/bot{token}/sendMessage", token)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(form)
                    .retrieve()
                    .toBodilessEntity();

        } catch (Exception e) {
            log.error("Ошибка отправки в Telegram: {}", e.getMessage(), e);
        }
    }
}
