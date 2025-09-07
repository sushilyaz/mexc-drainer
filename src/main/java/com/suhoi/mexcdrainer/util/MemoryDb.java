// src/main/java/com/suhoi/mexcdrainer/util/MemoryDb.java
package com.suhoi.mexcdrainer.util;

import com.suhoi.mexcdrainer.model.Creds;
import com.suhoi.mexcdrainer.model.DrainSession;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryDb {

    private static final Map<Long, Creds> accountA = new ConcurrentHashMap<>();
    private static final Map<Long, Creds> accountB = new ConcurrentHashMap<>();

    private static final Map<Long, AtomicBoolean> inProgress = new ConcurrentHashMap<>();
    private static final Map<Long, DrainSession> sessions = new ConcurrentHashMap<>();

    public static Creds getAccountA(Long chatId) { return accountA.get(chatId); }
    public static Creds getAccountB(Long chatId) { return accountB.get(chatId); }
    public static void setAccountA(Long chatId, Creds c) { accountA.put(chatId, c); }
    public static void setAccountB(Long chatId, Creds c) { accountB.put(chatId, c); }

    public static AtomicBoolean getFlag(Long chatId) {
        return inProgress.computeIfAbsent(chatId, id -> new AtomicBoolean(false));
    }

    public static DrainSession getSession(Long chatId) { return sessions.get(chatId); }
    public static void setSession(Long chatId, DrainSession s) { sessions.put(chatId, s); }
    public static void clearSession(Long chatId) { sessions.remove(chatId); }
}
