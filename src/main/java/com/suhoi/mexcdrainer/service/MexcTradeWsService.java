package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.Creds;
import com.suhoi.mexcdrainer.util.MemoryDb;
import com.suhoi.mexcdrainer.ws.MarketWsService;
import com.suhoi.mexcdrainer.ws.UserDataWsService;
import com.suhoi.mexcdrainer.ws.UserDataWsService.Side;
import com.suhoi.mexcdrainer.ws.dto.BookTicker;
import com.suhoi.mexcdrainer.ws.dto.OrderUpdate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Фасад для всего, что связано с WebSocket-ядром:
 * - публичные цены (bookTicker),
 * - приватные события (orders/deals/account),
 * - ожидание FILLED,
 * - кеш балансов,
 * - подготовка сокетов для чата.
 *
 * Идея: держим MexcTradeService "чистым" (REST, бизнес-логика),
 * а всю WS-интеграцию — здесь.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MexcTradeWsService {

    private final AppProperties props;
    private final MarketWsService marketWs;
    private final UserDataWsService userWs;

    /** Включено ли WS-ядро по фича-флагу. */
    public boolean wsEnabled() {
        return props.getDrain() != null
                && props.getDrain().getWs() != null
                && props.getDrain().getWs().isEnabled();
    }

    /**
     * Готовим WS для конкретного чата и символа:
     * - приватные сокеты A/B (по creds из MemoryDb),
     * - подписка на bookTicker символа.
     */
    public void prepareWs(long chatId, String symbol) {
        if (!wsEnabled()) return;
        final Creds ca = MemoryDb.getAccountA(chatId);
        final Creds cb = MemoryDb.getAccountB(chatId);
        if (ca != null) userWs.startForAccount(chatId, Side.A, ca);
        if (cb != null) userWs.startForAccount(chatId, Side.B, cb);
        marketWs.ensureSubscribed(symbol);
    }

    /** Топ-котировки из WS (если тик не «протух»). */
    public Optional<BookTicker> wsBest(String symbol) {
        if (!wsEnabled()) return Optional.empty();
        marketWs.ensureSubscribed(symbol);
        return marketWs.getBookTicker(symbol);
    }

    /** Подождать следующий тик (для «проклейки» книги) после собственной операции. */
    public boolean awaitNextTick(String symbol, long timeoutMs) {
        if (!wsEnabled()) return false;
        return marketWs.awaitNextTick(symbol, timeoutMs);
    }

    /** Ждать FILLED по ордеру на аккаунте A. */
    public CompletableFuture<OrderUpdate> awaitOrderFilledA(String symbol, String orderId, long timeoutMs, long chatId) {
        if (!wsEnabled()) return failed("UserData WS disabled");
        return userWs.awaitOrderFilled(chatId, Side.A, orderId, Duration.ofMillis(timeoutMs));
    }

    /** Ждать FILLED по ордеру на аккаунте B. */
    public CompletableFuture<OrderUpdate> awaitOrderFilledB(String symbol, String orderId, long timeoutMs, long chatId) {
        if (!wsEnabled()) return failed("UserData WS disabled");
        return userWs.awaitOrderFilled(chatId, Side.B, orderId, Duration.ofMillis(timeoutMs));
    }

    /** Кешированный баланс (free) по активу на аккаунте A. */
    public BigDecimal getCachedBalanceA(long chatId, String asset) {
        if (!wsEnabled()) return null;
        return userWs.getCachedBalance(chatId, Side.A, asset);
    }

    /** Кешированный баланс (free) по активу на аккаунте B. */
    public BigDecimal getCachedBalanceB(long chatId, String asset) {
        if (!wsEnabled()) return null;
        return userWs.getCachedBalance(chatId, Side.B, asset);
    }

    // ---------- хелперы ----------

    private static <T> CompletableFuture<T> failed(String msg) {
        var f = new CompletableFuture<T>();
        f.completeExceptionally(new IllegalStateException(msg));
        return f;
    }

    /** Из "ANTUSDT" → "ANT" (asset базового токена). */
    public static String baseAssetFromSymbol(String symbolUpper) {
        String s = symbolUpper.toUpperCase(Locale.ROOT);
        if (s.endsWith("USDT")) return s.substring(0, s.length() - 4);
        if (s.endsWith("USD"))  return s.substring(0, s.length() - 3);
        if (s.endsWith("BTC"))  return s.substring(0, s.length() - 3);
        if (s.endsWith("ETH"))  return s.substring(0, s.length() - 3);
        return s; // на крайний случай
    }
}
