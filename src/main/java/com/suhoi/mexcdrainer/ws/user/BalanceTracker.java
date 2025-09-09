package com.suhoi.mexcdrainer.ws.user;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BalanceTracker {

    @Value
    public static class BalanceKey {
        String apiKey;
        String asset; // "USDT" или "ANT" и т.п.
    }

    @Value
    public static class BalanceEntry {
        BigDecimal balance;    // available (без frozen), по ивентам MEXC — смотри PrivateAccountV3Api
        BigDecimal frozen;     // заморожено
        long updateTimeMs;     // время ивента
    }

    private final Map<BalanceKey, BalanceEntry> map = new ConcurrentHashMap<>();

    public void update(String apiKey, String asset, BigDecimal balance, BigDecimal frozen, long timeMs) {
        map.put(new BalanceKey(apiKey, asset), new BalanceEntry(balance, frozen, timeMs));
    }

    public BalanceEntry get(String apiKey, String asset) {
        return map.get(new BalanceKey(apiKey, asset));
    }
}
