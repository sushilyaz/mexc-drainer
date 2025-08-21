package com.suhoi.mexcdrainer.telegram;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class KeyStore {
    @Data
    @AllArgsConstructor
    public static class Keys {
        private String apiKey;
        private String secret;
    }

    // хранение по chatId (Long)
    private final Map<Long, Keys> accountA = new ConcurrentHashMap<>();
    private final Map<Long, Keys> accountB = new ConcurrentHashMap<>();

    public void setA(long chatId, String key, String secret) {
        accountA.put(chatId, new Keys(key, secret));
    }

    public void setB(long chatId, String key, String secret) {
        accountB.put(chatId, new Keys(key, secret));
    }

    public Keys getA(long chatId) {
        return accountA.get(chatId);
    }

    public Keys getB(long chatId) {
        return accountB.get(chatId);
    }
}
