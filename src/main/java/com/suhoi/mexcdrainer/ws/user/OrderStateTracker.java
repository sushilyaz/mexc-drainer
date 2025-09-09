package com.suhoi.mexcdrainer.ws.user;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

@Slf4j
public class OrderStateTracker {

    @Value
    public static class OrderState {
        String apiKey;
        String orderId;
        String clientId;
        String symbol;
        String status;        // MEXC: 1=NEW, 2=FILLED, 3=PARTIALLY, 4=CANCELED, 5=PARTIALLY_CANCELED (но мы приводим к текстам)
        BigDecimal executedQty;
        BigDecimal cummQuoteQty;
        BigDecimal avgPrice;
        long eventTime;
    }

    private static final OrderState UNKNOWN = new OrderState(null, null, null, null, "UNKNOWN",
            BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, 0L);

    private final Map<String, OrderState> lastByKey = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<OrderState>> waiters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timer = Executors.newScheduledThreadPool(1, r -> {
        Thread t = new Thread(r, "order-await");
        t.setDaemon(true); return t;
    });

    private static String k(String apiKey, String orderId) { return apiKey + "#" + orderId; }

    public void registerPlaced(String apiKey, String orderId, String clientId, String symbol) {
        lastByKey.put(k(apiKey, orderId), UNKNOWN);
        log.debug("[ORDER_EVENT] placed {} {} clientId={}", symbol, k(apiKey, orderId), clientId);
    }

    public void onOrderEvent(OrderState state) {
        String key = k(state.getApiKey(), state.getOrderId());
        lastByKey.put(key, state);
        // если финальное — завершим waiter
        if (isFinalStatus(state.getStatus())) {
            var f = waiters.remove(key);
            if (f != null && !f.isDone()) f.complete(state);
        }
    }

    public Optional<OrderState> get(String apiKey, String orderId) {
        return Optional.ofNullable(lastByKey.get(k(apiKey, orderId)));
    }

    public OrderState awaitFilledOrFinal(String apiKey, String orderId, long timeoutMs) {
        String key = k(apiKey, orderId);
        var last = lastByKey.get(key);
        if (last != null && isFinalStatus(last.getStatus())) return last;

        var cf = waiters.computeIfAbsent(key, k -> new CompletableFuture<>());
        // защита от вечного ожидания
        timer.schedule(() -> {
            if (!cf.isDone()) cf.complete(lastByKey.getOrDefault(key, UNKNOWN));
        }, timeoutMs, TimeUnit.MILLISECONDS);

        try {
            OrderState s = cf.get(timeoutMs + 1000, TimeUnit.MILLISECONDS);
            return s == null ? UNKNOWN : s;
        } catch (Exception e) {
            waiters.remove(key);
            return lastByKey.getOrDefault(key, UNKNOWN);
        }
    }

    private boolean isFinalStatus(String status) {
        if (status == null) return false;
        String s = status.toUpperCase();
        return s.contains("FILLED") || s.contains("CANCELED") || s.contains("REJECTED");
    }
}
