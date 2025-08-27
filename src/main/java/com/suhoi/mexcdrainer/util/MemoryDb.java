package com.suhoi.mexcdrainer.util;

import com.suhoi.mexcdrainer.model.Creds;
import lombok.*;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryDb {
    private static final Map<Long, Creds> accountA = new ConcurrentHashMap<>();
    private static final Map<Long, Creds> accountB = new ConcurrentHashMap<>();

    // Аккаунт A
    public static void setAccountA(Long chatId, Creds creds) {
        accountA.put(chatId, creds);
    }

    public static Creds getAccountA(Long chatId) {
        return accountA.get(chatId);
    }

    // Аккаунт B
    public static void setAccountB(Long chatId, Creds creds) {
        accountB.put(chatId, creds);
    }

    public static Creds getAccountB(Long chatId) {
        return accountB.get(chatId);
    }
}

