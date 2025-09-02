package com.suhoi.mexcdrainer.util;

import com.suhoi.mexcdrainer.model.Creds;
import com.suhoi.mexcdrainer.model.RangeState;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Простейшая in-memory "БД".
 * Теперь хранит не только ключи A/B, но и состояние диапазонного перелива на чат.
 */
public class MemoryDb {

    private static final Map<Long, Creds> accountA = new ConcurrentHashMap<>();
    private static final Map<Long, Creds> accountB = new ConcurrentHashMap<>();
    private static final Map<Long, RangeState> rangeState = new ConcurrentHashMap<>();

    // ===== Аккаунт A =====
    public static void setAccountA(Long chatId, Creds creds) {
        accountA.put(chatId, creds);
    }
    public static Creds getAccountA(Long chatId) {
        return accountA.get(chatId);
    }

    // ===== Аккаунт B =====
    public static void setAccountB(Long chatId, Creds creds) {
        accountB.put(chatId, creds);
    }
    public static Creds getAccountB(Long chatId) {
        return accountB.get(chatId);
    }

    // ===== Состояние диапазонного перелива =====
    public static RangeState getRangeState(Long chatId) {
        return rangeState.get(chatId);
    }

    public static void saveNewRangeState(Long chatId, RangeState state) {
        if (state != null) state.setUpdatedAt(Instant.now());
        rangeState.put(chatId, state);
    }

    public static void updateProgress(Long chatId, java.util.function.UnaryOperator<RangeState> updater) {
        rangeState.compute(chatId, (k, v) -> {
            RangeState cur = v == null ? new RangeState() : v;
            RangeState out = updater.apply(cur);
            if (out != null) out.setUpdatedAt(Instant.now());
            return out;
        });
    }

    public static void markPaused(Long chatId) {
        updateProgress(chatId, st -> {
            if (st == null) return null;
            st.setPaused(true);
            st.setRunning(false); // поток остановится на проверке paused
            return st;
        });
    }

    public static void clearRangeState(Long chatId) {
        rangeState.remove(chatId);
    }
}
