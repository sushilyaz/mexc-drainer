package com.suhoi.mexcdrainer.ws.market;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;

@Slf4j
public class OrderBook {

    private final String symbol;

    private final NavigableMap<BigDecimal, BigDecimal> bids = new TreeMap<>(reverseOrder());
    private final NavigableMap<BigDecimal, BigDecimal> asks = new TreeMap<>(naturalOrder());

    private volatile long lastUpdateTimeMs = 0;
    private volatile long lastVersion = 0L;
    private volatile boolean needsResync = false;

    public OrderBook(String symbol) {
        this.symbol = symbol;
    }

    public synchronized void clearAndApplySnapshot(NavigableMap<BigDecimal, BigDecimal> bidSnapshot,
                                                   NavigableMap<BigDecimal, BigDecimal> askSnapshot,
                                                   long sendTime) {
        bids.clear(); asks.clear();
        if (bidSnapshot != null) bids.putAll(bidSnapshot);
        if (askSnapshot != null) asks.putAll(askSnapshot);
        lastUpdateTimeMs = sendTime;
        lastVersion = 0L;
        needsResync = false;
        if (log.isDebugEnabled()) {
            log.debug("[WS_MARKET][SNAP_APPLIED] {} bids={} asks={} @{}", symbol, bids.size(), asks.size(),
                    Instant.ofEpochMilli(sendTime));
        }
    }

    public synchronized boolean applyIncrementWithVersions(QuoteDelta[] bidDeltas,
                                                           QuoteDelta[] askDeltas,
                                                           long fromVersion,
                                                           long toVersion,
                                                           long sendTime) {
        if (needsResync) {
            log.warn("[WS_MARKET][INC_IGNORED] {} needsResync=true (from={} to={})", symbol, fromVersion, toVersion);
            return false;
        }
        if (fromVersion <= 0 || toVersion < fromVersion) {
            log.warn("[WS_MARKET][INC_BAD_VER] {} from={} to={} — игнор", symbol, fromVersion, toVersion);
            return false;
        }

        if (lastVersion == 0L) {
            applyDeltas(bidDeltas, askDeltas);
            lastVersion = toVersion;
            lastUpdateTimeMs = sendTime;
            return true;
        }

        if (fromVersion != lastVersion + 1) {
            needsResync = true;
            log.warn("[WS_MARKET][INC_GAP] {} expected from={} got from={} (to={}) — needsResync=true",
                    symbol, lastVersion + 1, fromVersion, toVersion);
            return false;
        }

        applyDeltas(bidDeltas, askDeltas);
        lastVersion = toVersion;
        lastUpdateTimeMs = sendTime;
        return true;
    }

    public synchronized void applyIncrement(QuoteDelta[] bidDeltas,
                                            QuoteDelta[] askDeltas,
                                            long sendTime) {
        applyDeltas(bidDeltas, askDeltas);
        lastUpdateTimeMs = sendTime;
    }

    private void applyDeltas(QuoteDelta[] bidDeltas, QuoteDelta[] askDeltas) {
        if (bidDeltas != null) applySide(bids, bidDeltas);
        if (askDeltas != null) applySide(asks, askDeltas);
    }

    private void applySide(NavigableMap<BigDecimal, BigDecimal> side, QuoteDelta[] deltas) {
        for (QuoteDelta d : deltas) {
            if (d == null) continue;
            if (d.qty().signum() == 0) side.remove(d.price());
            else side.put(d.price(), d.qty());
        }
    }

    public synchronized Top top() {
        BigDecimal bid = bids.isEmpty() ? BigDecimal.ZERO : bids.firstKey();
        BigDecimal bidQty = bids.isEmpty() ? BigDecimal.ZERO : bids.firstEntry().getValue();
        BigDecimal ask = asks.isEmpty() ? BigDecimal.ZERO : asks.firstKey();
        BigDecimal askQty = asks.isEmpty() ? BigDecimal.ZERO : asks.firstEntry().getValue();
        return new Top(bid, bidQty, ask, askQty, lastUpdateTimeMs, lastVersion, needsResync);
    }

    /** Следующий по худшей цене bid относительно текущего топа (или ZERO, если нет). */
    public synchronized PriceLevel nextBidAfter(BigDecimal currentTopBid) {
        Map.Entry<BigDecimal, BigDecimal> e = bids.lowerEntry(currentTopBid);
        return (e == null) ? new PriceLevel(BigDecimal.ZERO, BigDecimal.ZERO) : new PriceLevel(e.getKey(), e.getValue());
    }

    /** Следующий по худшей цене ask относительно текущего топа (или ZERO, если нет). */
    public synchronized PriceLevel nextAskAfter(BigDecimal currentTopAsk) {
        Map.Entry<BigDecimal, BigDecimal> e = asks.higherEntry(currentTopAsk);
        return (e == null) ? new PriceLevel(BigDecimal.ZERO, BigDecimal.ZERO) : new PriceLevel(e.getKey(), e.getValue());
    }

    public boolean isStale(long staleMs, long nowMs) {
        return nowMs - lastUpdateTimeMs > staleMs;
    }

    public record QuoteDelta(BigDecimal price, BigDecimal qty) {}

    public record Top(BigDecimal bid, BigDecimal bidQty,
                      BigDecimal ask, BigDecimal askQty,
                      long lastUpdateTimeMs,
                      long lastVersion,
                      boolean needsResync) {
        public long stalenessMs(long now) { return Math.max(0, now - lastUpdateTimeMs); }
    }

    public record PriceLevel(BigDecimal price, BigDecimal qty) {}
}
