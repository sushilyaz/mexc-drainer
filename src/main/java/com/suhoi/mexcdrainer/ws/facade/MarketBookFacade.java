package com.suhoi.mexcdrainer.ws.facade;

import com.suhoi.mexcdrainer.config.WsMarketProperties;
import com.suhoi.mexcdrainer.ws.market.OrderBook;
import com.suhoi.mexcdrainer.ws.market.OrderBookRepository;
import com.suhoi.mexcdrainer.ws.user.OwnOrdersRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Optional;

import static java.math.BigDecimal.ZERO;

@Slf4j
@RequiredArgsConstructor
public class MarketBookFacade {

    private final OrderBookRepository books;
    private final OwnOrdersRegistry own;
    private final WsMarketProperties props;

    public record TopEx(BigDecimal bid, BigDecimal bidQty,
                        BigDecimal ask, BigDecimal askQty,
                        long updateTimeMs, long version, boolean needsResync) { }

    /** Инклюзивный топ (без self-exclusion). Удобен для ensure: сравнить свою цену с фактическим top. */
    public Optional<TopEx> topInclusive(String symbol) {
        var obOpt = books.get(symbol);
        if (obOpt.isEmpty()) return Optional.empty();
        var ob = obOpt.get();
        var t = ob.top();

        long now = System.currentTimeMillis();
        if (ob.isStale(props.getStaleMs(), now) || t.needsResync()) {
            log.warn("[WS_BOOK][STALE/RESYNC] {} staleness={}ms, ver={}, resync={}",
                    symbol, t.stalenessMs(now), t.lastVersion(), t.needsResync());
            return Optional.empty();
        }
        return Optional.of(new TopEx(t.bid(), t.bidQty(), t.ask(), t.askQty(), t.lastUpdateTimeMs(), t.lastVersion(), false));
    }

    /** Топ с исключением своих лимиток (только на первом уровне). Если свой остаток ≥ уровня — смещаем на соседний. */
    public Optional<TopEx> topExcludingSelfOnBest(String symbol, String apiKeySelf) {
        if (apiKeySelf == null) return topInclusive(symbol);

        var obOpt = books.get(symbol);
        if (obOpt.isEmpty()) return Optional.empty();
        var ob = obOpt.get();
        var t = ob.top();

        long now = System.currentTimeMillis();
        if (ob.isStale(props.getStaleMs(), now) || t.needsResync()) {
            log.warn("[WS_BOOK][STALE/RESYNC] {} staleness={}ms, ver={}, resync={}",
                    symbol, t.stalenessMs(now), t.lastVersion(), t.needsResync());
            return Optional.empty();
        }

        BigDecimal bid = t.bid(), bidQty = t.bidQty();
        BigDecimal ask = t.ask(), askQty = t.askQty();

        if (bid.signum() > 0) {
            BigDecimal ownBid = own.ownRemainingAtPrice(apiKeySelf, symbol, OwnOrdersRegistry.Side.BUY, bid);
            if (ownBid.signum() > 0) {
                BigDecimal rest = bidQty.subtract(ownBid);
                if (rest.signum() <= 0) {
                    OrderBook.PriceLevel nxt = ob.nextBidAfter(bid);
                    bid = nxt.price(); bidQty = nxt.qty();
                } else bidQty = rest;
            }
        }
        if (ask.signum() > 0) {
            BigDecimal ownAsk = own.ownRemainingAtPrice(apiKeySelf, symbol, OwnOrdersRegistry.Side.SELL, ask);
            if (ownAsk.signum() > 0) {
                BigDecimal rest = askQty.subtract(ownAsk);
                if (rest.signum() <= 0) {
                    OrderBook.PriceLevel nxt = ob.nextAskAfter(ask);
                    ask = nxt.price(); askQty = nxt.qty();
                } else askQty = rest;
            }
        }

        return Optional.of(new TopEx(bid, bidQty, ask, askQty, t.lastUpdateTimeMs(), t.lastVersion(), false));
    }

    /* ===== цены возле кромок (с guard) ===== */

    public Optional<BigDecimal> nearLowerSpread(String symbol, String apiKeySelf, BigDecimal guardFraction, BigDecimal tickSize) {
        var tOpt = topExcludingSelfOnBest(symbol, apiKeySelf);
        if (tOpt.isEmpty()) return Optional.empty();
        var t = tOpt.get();

        if (t.bid.signum() <= 0 && t.ask.signum() <= 0) {
            return Optional.of(minTick(tickSize));
        }
        BigDecimal spread = t.ask.subtract(t.bid);
        if (spread.signum() < 0) spread = ZERO;
        BigDecimal raw = t.bid.add(spread.multiply(nz(guardFraction)));
        BigDecimal aligned = alignCeil(raw, nz(tickSize));
        log.debug("[WS_BOOK][LOWER] {} bid={} ask={} spread={} guard={} -> raw={} aligned={} @{}",
                symbol, t.bid, t.ask, spread, guardFraction, raw, aligned, Instant.ofEpochMilli(t.updateTimeMs));
        return Optional.of(aligned);
    }

    public Optional<BigDecimal> nearUpperSpread(String symbol, String apiKeySelf, BigDecimal guardFraction, BigDecimal tickSize) {
        var tOpt = topExcludingSelfOnBest(symbol, apiKeySelf);
        if (tOpt.isEmpty()) return Optional.empty();
        var t = tOpt.get();

        if (t.bid.signum() <= 0 && t.ask.signum() <= 0) {
            return Optional.of(minTick(tickSize));
        }
        BigDecimal spread = t.ask.subtract(t.bid);
        if (spread.signum() < 0) spread = ZERO;
        BigDecimal raw = t.ask.subtract(spread.multiply(nz(guardFraction)));
        BigDecimal aligned = alignFloor(raw, nz(tickSize));
        log.debug("[WS_BOOK][UPPER] {} bid={} ask={} spread={} guard={} -> raw={} aligned={} @{}",
                symbol, t.bid, t.ask, spread, guardFraction, raw, aligned, Instant.ofEpochMilli(t.updateTimeMs));
        return Optional.of(aligned);
    }

    /* ===== утилиты ===== */

    private static BigDecimal nz(BigDecimal x) { return x == null ? ZERO : x; }
    private static BigDecimal minTick(BigDecimal tick) {
        return (tick != null && tick.signum() > 0) ? tick : new BigDecimal("0.00000001");
    }
    public static BigDecimal alignCeil(BigDecimal price, BigDecimal tick) {
        if (price == null || price.signum() <= 0) return ZERO;
        if (tick == null || tick.signum() <= 0) return price;
        BigDecimal steps = price.divide(tick, 0, RoundingMode.CEILING);
        return steps.multiply(tick);
    }
    public static BigDecimal alignFloor(BigDecimal price, BigDecimal tick) {
        if (price == null || price.signum() <= 0) return ZERO;
        if (tick == null || tick.signum() <= 0) return price;
        BigDecimal steps = price.divide(tick, 0, RoundingMode.FLOOR);
        return steps.multiply(tick);
    }
}
