package com.suhoi.mexcdrainer.ws.facade;


import com.suhoi.mexcdrainer.ws.user.BalanceTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * Берём баланс из приватного WS, если запись "свежая" (TTL).
 * Если запись устарела — возвращаем Optional.empty() (при интеграции подставишь REST).
 */
@Slf4j
@RequiredArgsConstructor
public class BalanceWsFacade {

    private final BalanceTracker balanceTracker;
    private final com.suhoi.mexcdrainer.config.WsUserProperties userProps;

    public Optional<BigDecimal> freshAvailable(String apiKey, String asset) {
        var e = balanceTracker.get(apiKey, asset);
        if (e == null) return Optional.empty();
        long age = System.currentTimeMillis() - e.getUpdateTimeMs();
        if (age > userProps.getBalanceFreshTtlMs()) {
            log.debug("[WS_USER][BALANCE_STALE] {} {} age={}ms > ttl={}ms",
                    hide(apiKey), asset, age, userProps.getBalanceFreshTtlMs());
            return Optional.empty();
        }
        // По ивентам MEXC (PrivateAccountV3Api) ты логировал balance (available) и frozen отдельно.
        return Optional.ofNullable(e.getBalance());
    }

    private static String hide(String apiKey) {
        if (apiKey == null || apiKey.length() < 6) return "***";
        return apiKey.substring(0,3) + "…" + apiKey.substring(apiKey.length()-3);
    }
}

