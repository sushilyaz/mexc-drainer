package com.suhoi.mexcdrainer.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.ws.dto.BookTicker;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.WebSocket;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class MarketWsService {

    private final AppProperties props;
    private final ObjectMapper om = new ObjectMapper();

    private static final String WS_ENDPOINT = "wss://wbs-api.mexc.com/ws";

    private volatile Inner client;

    private final ConcurrentMap<String, BookTicker> lastTicker = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CompletableFuture<BookTicker>> awaiters = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        // ленивый старт — ничего не делаем
    }

    public boolean isEnabled() {
        return props.getDrain() != null
                && props.getDrain().getWs() != null
                && props.getDrain().getWs().isEnabled();
    }

    /** Лениво подписываемся на bookTicker символа (idempotent). */
    public synchronized void ensureSubscribed(String symbol) {
        if (!isEnabled()) return;
        String ch = "spot@public.aggre.bookTicker.v3.api@10ms@" + symbol.toUpperCase(Locale.ROOT);

        if (client == null) {
            client = new Inner(WS_ENDPOINT, om);
            // 1) сначала регистрируем канал в subs (внутри add)
            client.subscribeIfNeeded(ch);
            // 2) только потом коннектимся без initialParams — onOpenHook ресабскраит всё из subs
            client.connect(null);
            log.info("[WS_MARKET_CONNECT+SUB] {}", ch);
        } else {
            client.subscribeIfNeeded(ch); // идемпотентно
        }
    }

    /** Последний тикер, если он моложе maxStalenessMs. */
    public Optional<BookTicker> getBookTicker(String symbol) {
        if (!isEnabled()) return Optional.empty();
        String key = symbol.toUpperCase(Locale.ROOT);
        BookTicker bt = lastTicker.get(key);
        if (bt == null) return Optional.empty();
        long staleness = System.currentTimeMillis() - bt.getSendTime();
        if (staleness > props.getDrain().getWs().getMaxStalenessMs()) return Optional.empty();
        return Optional.of(bt);
    }

    /** Дождаться СЛЕДУЮЩЕГО тика по символу (для «проклейки» книги). */
    public boolean awaitNextTick(String symbol, long timeoutMs) {
        if (!isEnabled()) return false;
        String key = symbol.toUpperCase(Locale.ROOT);
        CompletableFuture<BookTicker> fut = new CompletableFuture<>();
        awaiters.put(key, fut);
        try {
            ensureSubscribed(key);
            fut.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            log.warn("[WS_MARKET_WAIT_TICK_TIMEOUT] symbol={} t={}ms", symbol, timeoutMs);
            return false;
        } finally {
            awaiters.remove(key);
        }
    }

    // ==== Inner client ====
    private class Inner extends WsClient {
        private final Set<String> subs = ConcurrentHashMap.newKeySet();

        Inner(String url, ObjectMapper om) {
            super(url, om, props.getDrain().getWs().getPingIntervalMs());
        }

        void subscribeIfNeeded(String ch) {
            if (subs.add(ch)) {
                sendSub(List.of(ch));
                log.info("[WS_MARKET_SUB] {}", ch);
            }
        }

        @Override protected void onOpenHook(WebSocket ws) {
            sendSub(List.of(
                    "spot@private.orders.v3.api",
                    "spot@private.deals.v3.api",
                    "spot@private.account.v3.api"
            ));
        }

        @Override protected void onText(String text) {
            try {
                JsonNode j = om.readTree(text);
                if (!j.hasNonNull("channel")) return;

                String channel = j.get("channel").asText();
                if (!channel.startsWith("spot@public.aggre.bookTicker.v3.api.pb@")) return;

                String symbol = j.get("symbol").asText();
                JsonNode pbt = j.get("publicbookticker"); // проверь регистр в реальном стриме
                if (pbt == null) return;

                BookTicker bt = BookTicker.builder()
                        .symbol(symbol)
                        .bidPrice(new BigDecimal(pbt.path("bidprice").asText("0")))
                        .bidQuantity(new BigDecimal(pbt.path("bidquantity").asText("0")))
                        .askPrice(new BigDecimal(pbt.path("askprice").asText("0")))
                        .askQuantity(new BigDecimal(pbt.path("askquantity").asText("0")))
                        .sendTime(j.path("sendtime").asLong(System.currentTimeMillis()))
                        .build();

                lastTicker.put(symbol.toUpperCase(Locale.ROOT), bt);

                var waiter = awaiters.get(symbol.toUpperCase(Locale.ROOT));
                if (waiter != null && !waiter.isDone()) waiter.complete(bt);

            } catch (Exception e) {
                log.warn("[WS_MARKET_PARSE_ERR] {}", e.getMessage());
            }
        }

        @Override protected void onBinary(byte[] bytes) { /* no-op */ }
    }
}
