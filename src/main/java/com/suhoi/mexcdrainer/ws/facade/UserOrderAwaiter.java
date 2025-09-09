package com.suhoi.mexcdrainer.ws.facade;


import com.suhoi.mexcdrainer.ws.user.OrderStateTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Адаптер над OrderStateTracker: ждём финальный статус (FILLED/CANCELED/REJECTED)
 * через приватный WS. Если за timeout не дошло — вернём последнее известное.
 *
 * Фолбэк в REST сюда сознательно НЕ вшиваю — при интеграции ты подставишь
 * свой вызов (например, MexcTradeService::getOrder), а здесь оставлю только логику ожидания.
 */
@Slf4j
@RequiredArgsConstructor
public class UserOrderAwaiter {

    private final OrderStateTracker tracker;
    private final com.suhoi.mexcdrainer.config.WsUserProperties userProps;

    /**
     * Ждать финальный статус по ордеру.
     * @return последнее известное состояние (включая финальное), либо UNKNOWN.
     */
    public OrderStateTracker.OrderState awaitFinal(String apiKey, String orderId) {
        long timeoutMs = userProps.getAwaitFilledTimeoutMs();
        var s = tracker.awaitFilledOrFinal(apiKey, orderId, timeoutMs);
        log.debug("[WS_USER][AWAIT] {}#{} -> status={} filled={} cQuote={} avg={}",
                apiKeyHide(apiKey), orderId,
                s.getStatus(), s.getExecutedQty(), s.getCummQuoteQty(), s.getAvgPrice());
        return s;
    }

    /** Последнее известное (моментальный снимок), без ожидания. */
    public Optional<OrderStateTracker.OrderState> lastKnown(String apiKey, String orderId) {
        return tracker.get(apiKey, orderId);
    }

    private static String apiKeyHide(String apiKey) {
        if (apiKey == null || apiKey.length() < 6) return "***";
        return apiKey.substring(0,3) + "…" + apiKey.substring(apiKey.length()-3);
    }
}

