package com.suhoi.mexcdrainer.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.Creds;
import com.suhoi.mexcdrainer.ws.dto.AccountUpdate;
import com.suhoi.mexcdrainer.ws.dto.OrderUpdate;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import okhttp3.WebSocket;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MINUTES;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserDataWsService {

    public enum Side { A, B }

    private final AppProperties props;
    private final ObjectMapper om = new ObjectMapper();
    private final RestTemplate rest = new RestTemplate();

    private final Map<Key, Conn> conns = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, r -> {
        var t = new Thread(r, "mexc-userdata-keepalive");
        t.setDaemon(true);
        return t;
    });

    private static final String API_BASE = "https://api.mexc.com";
    private static final String WS_BASE  = "wss://wbs-api.mexc.com/ws";

    @Value
    private static class Key { long chatId; Side side; }

    private static class Conn {
        String listenKey;
        WsClient ws;
        Map<String, BigDecimal> balances = new ConcurrentHashMap<>(); // asset -> free
        Map<String, CompletableFuture<OrderUpdate>> waits = new ConcurrentHashMap<>(); // orderId -> future
        volatile long lastRecvMs;
        Creds creds;
    }

    public boolean isEnabled() {
        return props.getDrain() != null
                && props.getDrain().getWs() != null
                && props.getDrain().getWs().isEnabled();
    }

    /** Создаём/поднимаем приватный WS для чата/стороны. Идемпотентно. */
    public void startForAccount(long chatId, Side side, Creds creds) {
        if (!isEnabled()) return;
        var key = new Key(chatId, side);
        conns.computeIfAbsent(key, k -> createConn(k, creds));
    }

    /** Ожидание FILLED/CANCELED по ордеру. */
    public CompletableFuture<OrderUpdate> awaitOrderFilled(long chatId, Side side, String orderId, Duration timeout) {
        if (!isEnabled()) return failed("WS disabled");
        var c = conns.get(new Key(chatId, side));
        if (c == null) return failed("No WS connection for " + side);

        CompletableFuture<OrderUpdate> fut = new CompletableFuture<>();
        c.waits.put(orderId, fut);
        scheduler.schedule(() -> fut.completeExceptionally(new TimeoutException("awaitOrderFilled timeout")),
                timeout.toMillis(), TimeUnit.MILLISECONDS);
        return fut;
    }

    /** Кешированное значение free-баланса по активу. */
    public BigDecimal getCachedBalance(long chatId, Side side, String asset) {
        var c = conns.get(new Key(chatId, side));
        if (c == null) return null;
        return c.balances.getOrDefault(asset, null);
    }

    public boolean isHealthy(long chatId, Side side) {
        var c = conns.get(new Key(chatId, side));
        if (c == null) return false;
        long st = System.currentTimeMillis() - c.lastRecvMs;
        return st <= props.getDrain().getWs().getMaxStalenessMs() * 3L;
    }

    @PreDestroy
    void shutdown() {
        conns.values().forEach(c -> {
            try { c.ws.close(); } catch (Exception ignored) {}
            try { deleteListenKey(c.listenKey, c.creds); } catch (Exception ignored) {}
        });
        scheduler.shutdownNow();
    }

    // ===== Internal =====

    private Conn createConn(Key key, Creds creds) {
        String lk = createListenKey(creds);
        String url = WS_BASE + "?listenKey=" + lk;
        Conn c = new Conn();
        c.creds = creds;
        c.listenKey = lk;
        c.ws = new UserWs(url, om, key, c);
        c.ws.connect(List.of(
                "spot@private.orders.v3.api.pb",
                "spot@private.deals.v3.api.pb",
                "spot@private.account.v3.api.pb"
        ));
        c.lastRecvMs = System.currentTimeMillis();

        long refreshMin = Optional.ofNullable(props.getDrain().getWs().getPrivateCfg().getRefreshListenKeyMin()).orElse(25);
        scheduler.scheduleAtFixedRate(() -> {
            try { extendListenKey(c.listenKey, c.creds); } catch (Exception e) {
                log.warn("[LISTENKEY_EXTEND_ERR] {}", e.getMessage());
            }
        }, refreshMin, refreshMin, MINUTES);

        return c;
    }

    private String createListenKey(Creds creds) {
        try {
            HttpHeaders h = new HttpHeaders();
            h.add("X-MEXC-APIKEY", creds.getApiKey());
            h.setAccept(List.of(MediaType.APPLICATION_JSON));
            // ВАЖНО: тело как form-url-encoded (пусть пустое)
            h.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            RequestEntity<MultiValueMap<String,String>> req =
                    new RequestEntity<>(form, h, HttpMethod.POST,
                            URI.create(API_BASE + "/api/v3/userDataStream"));

            String body = rest.exchange(req, String.class).getBody();
            return om.readTree(body).get("listenKey").asText();
        } catch (Exception e) {
            throw new IllegalStateException("createListenKey failed: " + e.getMessage(), e);
        }
    }

    private void extendListenKey(String listenKey, Creds creds) {
        try {
            HttpHeaders h = new HttpHeaders();
            h.add("X-MEXC-APIKEY", creds.getApiKey());
            h.setAccept(List.of(MediaType.APPLICATION_JSON));
            h.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("listenKey", listenKey);

            RequestEntity<MultiValueMap<String,String>> req =
                    new RequestEntity<>(form, h, HttpMethod.PUT,
                            URI.create(API_BASE + "/api/v3/userDataStream"));

            rest.exchange(req, String.class);
        } catch (Exception e) {
            log.warn("[LISTENKEY_KEEPALIVE_ERR] {}", e.getMessage());
        }
    }

    private void deleteListenKey(String listenKey, Creds creds) {
        try {
            HttpHeaders h = new HttpHeaders();
            h.add("X-MEXC-APIKEY", creds.getApiKey());
            h.setAccept(List.of(MediaType.APPLICATION_JSON));
            h.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("listenKey", listenKey);

            RequestEntity<MultiValueMap<String,String>> req =
                    new RequestEntity<>(form, h, HttpMethod.DELETE,
                            URI.create(API_BASE + "/api/v3/userDataStream"));

            rest.exchange(req, String.class);
        } catch (Exception ignored) {}
    }

    private class UserWs extends WsClient {
        private final Key key;
        private final Conn conn;

        UserWs(String url, ObjectMapper om, Key key, Conn conn) {
            super(url, om, props.getDrain().getWs().getPingIntervalMs());
            this.key = key;
            this.conn = conn;
        }

        @Override protected void onOpenHook(WebSocket ws) {
            // Приватные каналы фиксированы — ресабскраим
            sendSub(List.of(
                    "spot@private.orders.v3.api.pb",
                    "spot@private.deals.v3.api.pb",
                    "spot@private.account.v3.api.pb"
            ));
        }

        @Override protected void onText(String text) {
            conn.lastRecvMs = System.currentTimeMillis();
            try {
                JsonNode j = om.readTree(text);
                if (!j.hasNonNull("channel")) return;
                String ch = j.get("channel").asText();

                switch (ch) {
                    case "spot@private.account.v3.api.pb" -> handleAccount(j);
                    case "spot@private.orders.v3.api.pb"  -> handleOrders(j);
                    case "spot@private.deals.v3.api.pb"   -> handleDeals(j);
                    default -> { /* ignore */ }
                }
            } catch (Exception e) {
                log.warn("[WS_PRIVATE_PARSE_ERR] {}", e.getMessage());
            }
        }

        @Override protected void onBinary(byte[] bytes) { /* no-op */ }

        private void handleAccount(JsonNode j) {
            JsonNode a = j.get("privateAccount");
            if (a == null) return;
            String asset = a.path("vcoinName").asText("");
            var upd = AccountUpdate.builder()
                    .asset(asset)
                    .balance(new BigDecimal(a.path("balanceAmount").asText("0")))
                    .frozen(new BigDecimal(a.path("frozenAmount").asText("0")))
                    .time(a.path("time").asLong(0))
                    .sendTime(j.path("sendTime").asLong(System.currentTimeMillis()))
                    .build();
            conn.balances.put(asset, upd.getBalance());
        }

        private void handleDeals(JsonNode j) {
            // Можно добавить лог/метрики — для awaitOrderFilled достаточно orders
        }

        private void handleOrders(JsonNode j) {
            JsonNode po = j.get("privateOrders");
            if (po == null) return;

            OrderUpdate upd = OrderUpdate.builder()
                    .symbol(j.path("symbol").asText(""))
                    .orderId(po.path("orderId").asText(null))
                    .clientOrderId(po.path("clientId").asText(null))
                    .price(new BigDecimal(po.path("price").asText("0")))
                    .quantity(new BigDecimal(po.path("quantity").asText("0")))
                    .avgPrice(new BigDecimal(po.path("avgPrice").asText("0")))
                    .cumulativeQuantity(new BigDecimal(po.path("cumulativeQuantity").asText("0")))
                    .status(po.path("status").asInt(0))
                    .sendTime(j.path("sendTime").asLong(System.currentTimeMillis()))
                    .build();

            completeIfMatch(upd.getOrderId(), upd);
            completeIfMatch(upd.getClientOrderId(), upd);
        }

        private void completeIfMatch(String keyOrderId, OrderUpdate upd) {
            if (keyOrderId == null) return;
            var fut = conn.waits.get(keyOrderId);
            if (fut != null && (upd.isFilled() || upd.isCanceled())) {
                fut.complete(upd);
                conn.waits.remove(keyOrderId);
            }
        }
    }

    private static <T> CompletableFuture<T> failed(String msg) {
        var f = new CompletableFuture<T>();
        f.completeExceptionally(new IllegalStateException(msg));
        return f;
    }
}
