package com.suhoi.mexcdrainer.ws.user;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Реестр собственных лимиток для self-exclusion.
 * Изменяемые поля НЕ final (иначе нельзя обновлять исполнение).
 */
public class OwnOrdersRegistry {

    public enum Side { BUY, SELL }

    public static final class OrderKey {
        private final String apiKey;
        private final String orderId;
        public OrderKey(String apiKey, String orderId) { this.apiKey = apiKey; this.orderId = orderId; }
        public String apiKey() { return apiKey; }
        public String orderId() { return orderId; }
        @Override public int hashCode() { return (apiKey == null ? 0 : apiKey.hashCode()) * 31 + (orderId == null ? 0 : orderId.hashCode()); }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OrderKey ok)) return false;
            return java.util.Objects.equals(apiKey, ok.apiKey) && java.util.Objects.equals(orderId, ok.orderId);
        }
    }

    public static final class OwnOrder {
        private final String apiKey;
        private final String symbol;
        private final Side side;
        private final BigDecimal price;
        private final BigDecimal origQty;
        private volatile BigDecimal executedQty; // меняется по мере исполнения
        private volatile boolean active;         // финальное состояние -> false

        public OwnOrder(String apiKey, String symbol, Side side, BigDecimal price, BigDecimal origQty) {
            this.apiKey = apiKey;
            this.symbol = symbol;
            this.side = side;
            this.price = price;
            this.origQty = origQty;
            this.executedQty = BigDecimal.ZERO;
            this.active = true;
        }
        public String apiKey() { return apiKey; }
        public String symbol() { return symbol; }
        public Side side() { return side; }
        public BigDecimal price() { return price; }
        public BigDecimal origQty() { return origQty; }
        public BigDecimal executedQty() { return executedQty; }
        public boolean active() { return active; }
        public void setExecutedQty(BigDecimal q) { this.executedQty = q; }
        public void setInactive() { this.active = false; }
    }

    private final Map<OrderKey, OwnOrder> byKey = new ConcurrentHashMap<>();

    public void register(String apiKey, String orderId, String symbol, Side side,
                         BigDecimal price, BigDecimal qty) {
        byKey.put(new OrderKey(apiKey, orderId), new OwnOrder(apiKey, symbol, side, price, qty));
    }

    public void markExecution(String apiKey, String orderId, BigDecimal cumulativeQty, boolean finalState) {
        var o = byKey.get(new OrderKey(apiKey, orderId));
        if (o != null) {
            if (cumulativeQty != null) o.setExecutedQty(cumulativeQty);
            if (finalState) o.setInactive();
        }
    }

    public BigDecimal ownRemainingAtPrice(String apiKey, String symbol, Side side, BigDecimal price) {
        BigDecimal sum = BigDecimal.ZERO;
        for (OwnOrder o : byKey.values()) {
            if (!o.active()) continue;
            if (!o.apiKey().equals(apiKey)) continue;
            if (!o.symbol().equalsIgnoreCase(symbol)) continue;
            if (o.side() != side) continue;
            if (o.price().compareTo(price) != 0) continue;
            BigDecimal rem = o.origQty().subtract(o.executedQty() == null ? BigDecimal.ZERO : o.executedQty());
            if (rem.signum() > 0) sum = sum.add(rem);
        }
        return sum;
    }

    public void finalizeOrder(String apiKey, String orderId) {
        var o = byKey.get(new OrderKey(apiKey, orderId));
        if (o != null) o.setInactive();
    }
}
