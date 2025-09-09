package com.suhoi.mexcdrainer.ws.market;

import com.mxc.push.common.protobuf.PublicAggreDepthV3ApiItem;
import com.mxc.push.common.protobuf.PublicIncreaseDepthV3ApiItem;
import com.mxc.push.common.protobuf.PublicLimitDepthV3ApiItem;
import com.suhoi.mexcdrainer.config.WsMarketProperties;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Репозиторий локальных книг. Умеет собирать снапшот и применять инкременты.
 * Для инкрементов есть две формы: Aggre (с версиями) и Increase (fallback, без версий).
 */
@Slf4j
public class OrderBookRepository {

    private final Map<String, OrderBook> books = new ConcurrentHashMap<>();
    private final WsMarketProperties props;

    public OrderBookRepository(WsMarketProperties props) {
        this.props = props;
    }

    public OrderBook getOrCreate(String symbol) {
        return books.computeIfAbsent(symbol, OrderBook::new);
    }

    public Optional<OrderBook> get(String symbol) {
        return Optional.ofNullable(books.get(symbol));
    }

    /* ================= public API: от MarketWsService ================= */

    public void applySnapshot(String symbol,
                              NavigableMap<BigDecimal, BigDecimal> bidSnapshot,
                              NavigableMap<BigDecimal, BigDecimal> askSnapshot,
                              long sendTime) {
        getOrCreate(symbol).clearAndApplySnapshot(bidSnapshot, askSnapshot, sendTime);
        log.debug("[WS_MARKET][SNAPSHOT] {} bids={} asks={}", symbol,
                bidSnapshot == null ? 0 : bidSnapshot.size(),
                askSnapshot == null ? 0 : askSnapshot.size());
    }

    public boolean applyIncrementWithVersions(String symbol,
                                              OrderBook.QuoteDelta[] bidDeltas,
                                              OrderBook.QuoteDelta[] askDeltas,
                                              long fromVersion,
                                              long toVersion,
                                              long sendTime) {
        return getOrCreate(symbol).applyIncrementWithVersions(bidDeltas, askDeltas, fromVersion, toVersion, sendTime);
    }

    public void applyIncrementFallback(String symbol,
                                       OrderBook.QuoteDelta[] bidDeltas,
                                       OrderBook.QuoteDelta[] askDeltas,
                                       long sendTime) {
        getOrCreate(symbol).applyIncrement(bidDeltas, askDeltas, sendTime);
    }

    /* ================= helpers: сборка структур из proto ================= */

    /** Построение snapshot-карты из proto-элементов partial depth. */
    public static NavigableMap<BigDecimal, BigDecimal> map(boolean bids, List<PublicLimitDepthV3ApiItem> levels) {
        final java.util.Comparator<BigDecimal> cmp = bids
                ? java.util.Comparator.reverseOrder()
                : java.util.Comparator.naturalOrder();
        // TreeMap: нам хватает однопоточной сборки снапшота
        NavigableMap<BigDecimal, BigDecimal> m = new TreeMap<>(cmp);
        if (levels != null) {
            for (PublicLimitDepthV3ApiItem it : levels) {
                try {
                    BigDecimal price = new BigDecimal(it.getPrice());
                    BigDecimal qty   = new BigDecimal(it.getQuantity());
                    if (qty.signum() != 0) m.put(price, qty);
                } catch (Exception ignore) { /* защитный парсинг */ }
            }
        }
        return m;
    }

    /** Конвертация INCREASE-инкрементов (без версий) в QuoteDelta[]. */
    public static OrderBook.QuoteDelta[] toIncreaseDeltas(List<PublicIncreaseDepthV3ApiItem> items) {
        if (items == null || items.isEmpty()) return new OrderBook.QuoteDelta[0];
        return items.stream()
                .map(it -> new OrderBook.QuoteDelta(bd(it.getPrice()), bd(it.getQuantity())))
                .toArray(OrderBook.QuoteDelta[]::new);
    }

    /** Конвертация AGGRE-инкрементов (с версиями) в QuoteDelta[]. */
    public static OrderBook.QuoteDelta[] toAggreDeltas(List<PublicAggreDepthV3ApiItem> items) {
        if (items == null || items.isEmpty()) return new OrderBook.QuoteDelta[0];
        return items.stream()
                .map(it -> new OrderBook.QuoteDelta(bd(it.getPrice()), bd(it.getQuantity())))
                .toArray(OrderBook.QuoteDelta[]::new);
    }

    private static BigDecimal bd(String s) {
        try { return new BigDecimal(s); } catch (Exception e) { return BigDecimal.ZERO; }
    }
}
