package com.suhoi.mexcdrainer.ws.market;

import com.mxc.push.common.protobuf.PublicAggreBookTickerV3Api;
import com.mxc.push.common.protobuf.PublicAggreDepthsV3Api;
import com.mxc.push.common.protobuf.PublicIncreaseDepthsV3Api;
import com.suhoi.mexcdrainer.config.WsMarketProperties;
import com.suhoi.mexcdrainer.ws.MexcWsClient;
import com.suhoi.mexcdrainer.util.MexcChannels;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Подписывается на bookTicker + partial depth + aggre (diff) depth и поддерживает локальные книги.
 * Контроль версий: из PublicAggreDepthsV3Api (fromVersion/toVersion).
 */
@Slf4j
@RequiredArgsConstructor
public class MarketWsService implements MexcWsClient.Listener {

    private final MexcWsClient client;
    private final OrderBookRepository repo;
    private final WsMarketProperties props;

    private final Set<String> subscribed = ConcurrentHashMap.newKeySet();

    @PostConstruct
    public void start() {
        client.addListener(this);
        log.info("[WS_MARKET] started");
    }

    @PreDestroy
    public void stop() {
        client.removeListener(this);
        log.info("[WS_MARKET] stopped");
    }

    /** Гарантирует подписки по символу. */
    public void ensureSubscribed(String symbol) {
        String key = symbol.toUpperCase(Locale.ROOT);
        if (subscribed.add(key)) {
            client.subscribe(
                    MexcChannels.bookTicker(key, props.getIntervalMs()),
                    MexcChannels.partialDepth(key, props.getPartialLevels()),
                    MexcChannels.diffDepth(key, props.getIntervalMs()) // это aggre.depth
            );
            log.info("[WS_MARKET][SUB] {} ({}ms, partial={})", key, props.getIntervalMs(), props.getPartialLevels());
        }
    }

    /* ===== MexcWsClient.Listener ===== */

    @Override public void onOpen() { log.info("✅ WS_MARKET OPEN"); }

    @Override
    public void onBookTicker(String symbol, PublicAggreBookTickerV3Api bt, long ts) {
        if (log.isDebugEnabled()) {
            log.debug("[WS_MARKET][BT] {} bid={} ask={} @{}", symbol, bt.getBidPrice(), bt.getAskPrice(), ts);
        }
    }

    /** partial depth snapshot */
    @Override
    public void onLimitDepth(String symbol, com.mxc.push.common.protobuf.PublicLimitDepthsV3Api depth, long ts) {
        try {
            var bids = OrderBookRepository.map(true, depth.getBidsList());
            var asks = OrderBookRepository.map(false, depth.getAsksList());
            repo.applySnapshot(symbol, bids, asks, ts);
            log.info("[WS_MARKET][SNAP] {} bids={} asks={}", symbol, bids.size(), asks.size());
        } catch (Exception e) {
            log.warn("[WS_MARKET][SNAP_ERR] {} {}", symbol, e.toString());
        }
    }

    /** aggre depth increments — здесь есть fromVersion/toVersion (как строки в proto) */
    @Override
    public void onAggreDepths(String symbol, PublicAggreDepthsV3Api ag, long ts) {
        try {
            long fromV = safeLong(ag.getFromVersion(), -1L);
            long toV   = safeLong(ag.getToVersion(),   -1L);
            var bidD   = OrderBookRepository.toAggreDeltas(ag.getBidsList()); // <-- ВАЖНО
            var askD   = OrderBookRepository.toAggreDeltas(ag.getAsksList()); // <-- ВАЖНО

            boolean ok = repo.applyIncrementWithVersions(symbol, bidD, askD, fromV, toV, ts);
            if (log.isDebugEnabled()) {
                log.debug("[WS_MARKET][INC_AGG] {} from={} to={} ok={} (+{} bids, +{} asks)",
                        symbol, fromV, toV, ok, bidD.length, askD.length);
            }
        } catch (Exception e) {
            log.warn("[WS_MARKET][INC_AGG_ERR] {} {}", symbol, e.toString());
        }
    }

    /** fallback для PublicIncreaseDepthsV3Api — на случай старого протокола */
    @Override
    public void onDepthInc(String symbol, PublicIncreaseDepthsV3Api inc, long ts) {
        try {
            var bidD = OrderBookRepository.toIncreaseDeltas(inc.getBidsList()); // <-- ВАЖНО
            var askD = OrderBookRepository.toIncreaseDeltas(inc.getAsksList()); // <-- ВАЖНО
            repo.applyIncrementFallback(symbol, bidD, askD, ts);
            if (log.isDebugEnabled()) {
                log.debug("[WS_MARKET][INC_FALLBACK] {} (+{} bids, +{} asks) @{}", symbol, bidD.length, askD.length, ts);
            }
        } catch (Exception e) {
            log.warn("[WS_MARKET][INC_FB_ERR] {} {}", symbol, e.toString());
        }
    }

    /* ===== util ===== */

    /** Безопасный парсер long из строки (протобуф даёт from/to как строковые числа). */
    private static long safeLong(String s, long def) {
        if (s == null) return def;
        try { return Long.parseLong(s.trim()); } catch (Exception ignore) { return def; }
    }
}
