package com.suhoi.mexcdrainer.util;

import com.suhoi.mexcdrainer.model.Creds;
import com.suhoi.mexcdrainer.model.RangeState;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

public final class MemoryDb {
    private MemoryDb() {}

    private static final Map<Long, Creds> A = new ConcurrentHashMap<>();
    private static final Map<Long, Creds> B = new ConcurrentHashMap<>();
    private static final Map<Long, RangeState> RANGE = new ConcurrentHashMap<>();

    public static void setAccountA(long chatId, Creds c) { A.put(chatId, c); }
    public static void setAccountB(long chatId, Creds c) { B.put(chatId, c); }
    public static Creds getAccountA(long chatId) { return A.get(chatId); }
    public static Creds getAccountB(long chatId) { return B.get(chatId); }

    public static void saveNewRangeState(Long chatId, RangeState s) {
        if (s != null) {
            s.setChatId(chatId);
            s.setUpdatedAt(Instant.now());
            RANGE.put(chatId, s);
        }
    }

    public static RangeState getRangeState(Long chatId) {
        return RANGE.get(chatId);
    }

    /** Универсальное атомарное обновление состояния. */
    public static void updateProgress(Long chatId, UnaryOperator<RangeState> mut) {
        RANGE.compute(chatId, (k, old) -> {
            RangeState next = mut.apply(old);
            if (next != null) next.setUpdatedAt(Instant.now());
            return next;
        });
    }

    /** Поставить паузу (без отмены ордеров — это обязанность сервиса логики). */
    public static void markPaused(Long chatId) {
        updateProgress(chatId, st -> {
            if (st == null) return null;
            st.setPaused(true);
            st.setRunning(false);
            return st;
        });
    }
}
