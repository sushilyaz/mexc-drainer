package com.suhoi.mexcdrainer.ws.market;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mxc.push.common.protobuf.PublicAggreDepthsV3Api;
import com.mxc.push.common.protobuf.PublicBookTickerV3Api;
import com.mxc.push.common.protobuf.PublicIncreaseDepthsV3Api;
import com.mxc.push.common.protobuf.PublicLimitDepthsV3Api;
import com.mxc.push.common.protobuf.PushDataV3ApiWrapper;
import com.suhoi.mexcdrainer.ws.WsConnectionManager;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Market WS-сервис для получения bookTicker и глубины книги (snapshot + increments),
 * со строгой проверкой версий и авто-ресинком при гэпах.
 *
 * Особенности:
 *  - Подписка на:
 *      * spot@public.aggre.bookTicker.v3.api.pb@{interval}@{symbol}
 *      * spot@public.limit.depth.v3.api.pb@{interval}@{symbol}        (снапшот)
 *      * spot@public.aggre.depth.v3.api.pb@{interval}@{symbol}        (инкременты с from/to)
 *    Опционально:
 *      * spot@public.increase.depth.v3.api.pb@{interval}@{symbol}     (инкременты только с version)
 *  - Локальная книга на основе TreeMap (L2), хранится на символ.
 *  - При потере пакетов (fromVersion != last+1, либо version не монотонен) — триггерим реснапшот.
 *  - WsConnectionManager выполняет auto-reconnect и re-subscribe (мы помним каналы).
 */
@Slf4j
public class MarketWsService implements AutoCloseable {

    /** Текущий top-of-book (агрегированный) по символу — из bookTicker. */
    private final Map<String, Top> topBySymbol = new ConcurrentHashMap<>();
    /** Локальные книги по символу. */
    private final Map<String, OrderBook> books = new ConcurrentHashMap<>();
    /** Запоминаем, с каким interval подписывались на depth по каждому символу — нужно для ресинка. */
    private final Map<String, String> depthIntervalBySymbol = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Top>> oneShotWaiters = new ConcurrentHashMap<>();

    /** Значение по умолчанию для interval глубины — на случай, если карта пуста. */
    private volatile String defaultDepthInterval = "10ms";

    private WsConnectionManager ws;

    /**
     * Инициализация ws-клиента. Подписки делаются отдельными вызовами subscribe*().
     */
    public void start(String wsPublicUrl,
                      String intervalBookTicker,
                      String intervalDepth,
                      int pingMs,
                      int wsPingFrameMs,
                      int reconnectBase,
                      int reconnectMax) {

        this.defaultDepthInterval = intervalDepth != null ? intervalDepth : this.defaultDepthInterval;

        this.ws = WsConnectionManager.builder()
                .uri(URI.create(wsPublicUrl))
                .textHandler(this::onText)
                .binaryHandler(this::onBinary)
                .openHandler(() -> log.info("MarketWS opened"))
                .closeHandler((code, reason) -> {
                    log.warn("MarketWS closed code={} reason={}", code, reason);
                    // WsConnectionManager сам пересабскрайбит запомненные каналы после reconnection:
                    ws.connectWithRetry(reconnectBase, reconnectMax);
                })
                .errorHandler(err -> log.warn("MarketWS error: {}", err.toString()))
                .pingIntervalMs(pingMs)
                .wsPingFrameIntervalMs(wsPingFrameMs)
                .build();

        ws.connectWithRetry(reconnectBase, reconnectMax);
    }
    public CompletableFuture<Top> nextTopUpdate(String symbol) {
        var f = new CompletableFuture<Top>();
        oneShotWaiters.put(symbol, f);
        return f;
    }
    // ==========================
    // Подписки
    // ==========================

    /** Подписка на агрегированный топ книги (bid/ask). */
    public void subscribeBookTicker(String symbol, String interval) {
        String ch = "spot@public.aggre.bookTicker.v3.api.pb@" + interval + "@" + symbol;
        ws.rememberSubscription(ch);
        ws.sendText("""
            {"method":"SUBSCRIPTION","params":["%s"]}
            """.formatted(ch));
    }

    /**
     * Подписка на снапшот + инкременты для книги.
     * ВАЖНО: сначала limit.depth (даёт полный снимок и его version), затем aggre.depth (from/to).
     */
    public void subscribeDepthWithSnapshot(String symbol, String interval) {
        String effInterval = interval != null ? interval : defaultDepthInterval;
        depthIntervalBySymbol.put(symbol, effInterval);
        subscribeLimitDepth(symbol, effInterval);
        subscribeAggreDepth(symbol, effInterval);
    }

    /** Только снапшот limit.depth. Обычно вызывается внутри subscribeDepthWithSnapshot или при ресинке. */
    public void subscribeLimitDepth(String symbol, String interval) {
        String ch = "spot@public.limit.depth.v3.api.pb@" + interval + "@" + symbol;
        ws.rememberSubscription(ch);
        ws.sendText("""
            {"method":"SUBSCRIPTION","params":["%s"]}
            """.formatted(ch));
    }

    /** Только инкременты aggregated depth (с fromVersion/toVersion). */
    public void subscribeAggreDepth(String symbol, String interval) {
        String ch = "spot@public.aggre.depth.v3.api.pb@" + interval + "@" + symbol;
        ws.rememberSubscription(ch);
        ws.sendText("""
            {"method":"SUBSCRIPTION","params":["%s"]}
            """.formatted(ch));
    }

    /** (Опционально) Инкременты increase.depth (только version). */
    public void subscribeIncreaseDepth(String symbol, String interval) {
        String ch = "spot@public.increase.depth.v3.api.pb@" + interval + "@" + symbol;
        ws.rememberSubscription(ch);
        ws.sendText("""
            {"method":"SUBSCRIPTION","params":["%s"]}
            """.formatted(ch));
    }

    // ==========================
    // Доступ к данным
    // ==========================

    /** Последний известный top-of-book из bookTicker. */
    public Top getTop(String symbol) {
        return topBySymbol.get(symbol);
    }

    /** Лучший bid по локальной книге (если снапшот уже был применён). */
    public BigDecimal getBestBid(String symbol) {
        OrderBook ob = books.get(symbol);
        return ob == null ? BigDecimal.ZERO : ob.bestBid();
    }

    /** Лучший ask по локальной книге (если снапшот уже был применён). */
    public BigDecimal getBestAsk(String symbol) {
        OrderBook ob = books.get(symbol);
        return ob == null ? BigDecimal.ZERO : ob.bestAsk();
    }

    // ==========================
    // WS handlers
    // ==========================

    private void onText(String t) {
        // Здесь прилетают ответы на SUBSCRIPTION/UNSUBSCRIPTION/ACK/PONG — по желанию можно фильтровать.
        log.debug("MarketWS TEXT: {}", t);
    }

    private void onBinary(byte[] bytes) {
        try {
            PushDataV3ApiWrapper w = PushDataV3ApiWrapper.parseFrom(bytes);
            String ch = w.getChannel();
            String symbol = w.getSymbol();

            if (ch.contains("bookTicker")) {
                // Агрегированный тикер книги (быстрый топ)
                PublicBookTickerV3Api bt = w.getPublicBookTicker();
                Top top = new Top(
                        new BigDecimal(bt.getBidPrice()),
                        new BigDecimal(bt.getAskPrice()),
                        new BigDecimal(bt.getBidQuantity()),
                        new BigDecimal(bt.getAskQuantity()),
                        w.getSendTime()
                );
                var waiter = oneShotWaiters.remove(symbol);
                if (waiter != null) {
                    waiter.complete(top);
                }
                topBySymbol.put(symbol, top);
                return;
            }

            if (ch.contains("limit.depth")) {
                // Полный снимок книги + версия
                PublicLimitDepthsV3Api snap = w.getPublicLimitDepths();
                books.computeIfAbsent(symbol, s -> new OrderBook()).applySnapshot(snap);
                return;
            }

            if (ch.contains("aggre.depth")) {
                // Инкременты с from/to — предпочтительный канал
                PublicAggreDepthsV3Api inc = w.getPublicAggreDepths();
                OrderBook ob = books.computeIfAbsent(symbol, s -> new OrderBook());
                boolean ok = ob.applyAggre(inc);
                if (!ok) requestResyncSnapshot(symbol);
                return;
            }

            if (ch.contains("increase.depth")) {
                // Опциональный канал инкрементов — только version (монотонная проверка)
                PublicIncreaseDepthsV3Api inc = w.getPublicIncreaseDepths();
                OrderBook ob = books.computeIfAbsent(symbol, s -> new OrderBook());
                boolean ok = ob.applyIncrease(inc);
                if (!ok) requestResyncSnapshot(symbol);
            }

        } catch (InvalidProtocolBufferException e) {
            log.warn("MarketWS binary parse error: {}", e.getMessage());
        } catch (Exception e) {
            log.warn("MarketWS onBinary error: {}", e.getMessage(), e);
        }
    }

    /**
     * Ресинк книги при обнаружении гэпа: запрашиваем новый снапшот тем же interval.
     * Инкременты продолжат применяться к новой базе.
     */
    private void requestResyncSnapshot(String symbol) {
        String interval = depthIntervalBySymbol.getOrDefault(symbol, defaultDepthInterval);
        log.warn("Requesting re-sync snapshot for {} with interval {}", symbol, interval);
        subscribeLimitDepth(symbol, interval);
    }

    @Override
    public void close() {
        try { if (ws != null) ws.close(); } catch (Exception ignore) {}
    }

    // ==========================
    // Внутренние модели/структуры
    // ==========================

    @Value
    public static class Top {
        BigDecimal bid;
        BigDecimal ask;
        BigDecimal bidQty;
        BigDecimal askQty;
        long ts;
    }

    /**
     * Упрощённая L2-книга с версионностью:
     *  - applySnapshot — полный снимок и установка version.
     *  - applyAggre   — инкременты с fromVersion/toVersion (строгая непрерывность).
     *  - applyIncrease— инкременты с одиночным version (монотонность).
     */
    private static class OrderBook {
        private final TreeMap<BigDecimal, BigDecimal> bids = new TreeMap<>(Comparator.reverseOrder());
        private final TreeMap<BigDecimal, BigDecimal> asks = new TreeMap<>(Comparator.naturalOrder());
        /** Последняя применённая версия. */
        private volatile long version;

        /** Полный снапшот limit.depth. */
        synchronized void applySnapshot(PublicLimitDepthsV3Api snap) {
            asks.clear();
            bids.clear();

            snap.getAsksList().forEach(l -> {
                BigDecimal p = new BigDecimal(l.getPrice());
                BigDecimal q = new BigDecimal(l.getQuantity());
                if (q.signum() > 0) asks.put(p, q);
            });
            snap.getBidsList().forEach(l -> {
                BigDecimal p = new BigDecimal(l.getPrice());
                BigDecimal q = new BigDecimal(l.getQuantity());
                if (q.signum() > 0) bids.put(p, q);
            });

            version = parseLongSafe(snap.getVersion()); // версия снимка
            log.debug("Snapshot applied: ver={} | bids={} asks={}", version, bids.size(), asks.size());
        }

        /**
         * Инкременты aggregated depth — строгая проверка непрерывности по from/to.
         * @return true — инкремент применён; false — обнаружен гэп (нужно ресинхронизироваться)
         */
        synchronized boolean applyAggre(PublicAggreDepthsV3Api ag) {
            long from = parseLongSafe(ag.getFromVersion());
            long to   = parseLongSafe(ag.getToVersion());

            if (version != 0 && from != version + 1) {
                log.warn("Depth gap (aggre): expected from={} but got {}", version + 1, from);
                return false;
            }

            ag.getAsksList().forEach(l -> {
                BigDecimal p = new BigDecimal(l.getPrice());
                BigDecimal q = new BigDecimal(l.getQuantity());
                if (q.signum() == 0) asks.remove(p); else asks.put(p, q);
            });
            ag.getBidsList().forEach(l -> {
                BigDecimal p = new BigDecimal(l.getPrice());
                BigDecimal q = new BigDecimal(l.getQuantity());
                if (q.signum() == 0) bids.remove(p); else bids.put(p, q);
            });

            version = to;
            return true;
        }

        /**
         * Инкременты increase depth — проверяем только монотонный growth версии.
         * @return true — применено; false — версия не соответствует ожидаемой (нужен ресинк)
         */
        synchronized boolean applyIncrease(PublicIncreaseDepthsV3Api inc) {
            long v = parseLongSafe(inc.getVersion());
            if (version != 0 && v != version + 1) {
                log.warn("Depth gap (increase): expected ver={} but got {}", version + 1, v);
                return false;
            }

            inc.getAsksList().forEach(l -> {
                BigDecimal p = new BigDecimal(l.getPrice());
                BigDecimal q = new BigDecimal(l.getQuantity());
                if (q.signum() == 0) asks.remove(p); else asks.put(p, q);
            });
            inc.getBidsList().forEach(l -> {
                BigDecimal p = new BigDecimal(l.getPrice());
                BigDecimal q = new BigDecimal(l.getQuantity());
                if (q.signum() == 0) bids.remove(p); else bids.put(p, q);
            });

            version = v;
            return true;
        }

        BigDecimal bestBid() { return bids.isEmpty() ? BigDecimal.ZERO : bids.firstKey(); }
        BigDecimal bestAsk() { return asks.isEmpty() ? BigDecimal.ZERO : asks.firstKey(); }

        private static long parseLongSafe(String s) {
            try {
                return Long.parseLong((s == null || s.isEmpty()) ? "0" : s);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
    }
}
