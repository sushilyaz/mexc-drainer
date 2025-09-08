package com.suhoi.mexcdrainer.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.Creds;
import com.suhoi.mexcdrainer.ws.dto.AccountUpdate;
import com.suhoi.mexcdrainer.ws.dto.OrderUpdate;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import okhttp3.WebSocket;
import org.springframework.http.*;
import org.springframework.http.client.BufferingClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Приватный UserData WebSocket + обёртки над /api/v3/userDataStream.
 * ВАЖНО: Эндпоинты userDataStream на MEXC — SIGNED. Подписываем query-string и НЕ шлём тела.
 */
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

    // Базовые URL (REST берём из конфига, WS — фиксированный wss)
    private String apiBase() { return Optional.ofNullable(props.getMexc()).map(AppProperties.Mexc::getBaseUrl).orElse("https://api.mexc.com"); }
    private static final String WS_BASE = "wss://wbs-api.mexc.com/ws";

    @Value
    private static class Key { long chatId; Side side; }

    // --- Синхронизация времени (чтобы timestamp попадал в recvWindow) ---
    private static final long TIME_SYNC_TTL_MS = 5 * 60_000L;
    private final AtomicLong timeOffsetMs = new AtomicLong(0L);
    private final AtomicLong timeSyncedAtMs = new AtomicLong(0L);

    private long nowWithOffset() {
        long now = System.currentTimeMillis();
        if (now - timeSyncedAtMs.get() > TIME_SYNC_TTL_MS) {
            syncServerTimeQuiet();
        }
        return now + timeOffsetMs.get();
    }

    private void syncServerTimeQuiet() {
        try {
            // unsigned GET /api/v3/time
            URI uri = UriComponentsBuilder.fromHttpUrl(apiBase() + "/api/v3/time").build(true).toUri();
            ResponseEntity<String> resp = rest.exchange(new RequestEntity<Void>(new HttpHeaders(), HttpMethod.GET, uri), String.class);
            JsonNode j = om.readTree(resp.getBody());
            long serverTime = j.path("serverTime").asLong(0L);
            long now = System.currentTimeMillis();
            long offset = serverTime - now;
            timeOffsetMs.set(offset);
            timeSyncedAtMs.set(now);
            log.info("[TIME_SYNC] serverTime={}, localNow={}, offsetMs={}", serverTime, now, offset);
        } catch (Exception e) {
            // не фатально — продолжим с локальным временем
            log.debug("[TIME_SYNC_ERR] {}", e.getMessage());
            timeSyncedAtMs.set(System.currentTimeMillis());
        }
    }

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

    /* =============================== SIGNED REST for userDataStream =============================== */

    /** Создание listenKey: POST /api/v3/userDataStream (SIGNED, без тела). */
    private String createListenKey(Creds creds) {
        try {
            JsonNode j = signedUserData("POST", "/api/v3/userDataStream", new LinkedHashMap<>(), creds);
            String lk = j.path("listenKey").asText(null);
            if (lk == null || lk.isBlank()) {
                throw new IllegalStateException("listenKey пустой в ответе: " + j.toPrettyString());
            }
            log.info("<< listenKey={}", lk);
            return lk;
        } catch (Exception e) {
            throw new IllegalStateException("createListenKey failed: " + e.getMessage(), e);
        }
    }

    /** Продление listenKey: PUT /api/v3/userDataStream?listenKey=... (SIGNED, без тела). */
    private void extendListenKey(String listenKey, Creds creds) {
        try {
            Map<String, String> p = new LinkedHashMap<>();
            p.put("listenKey", listenKey);
            signedUserData("PUT", "/api/v3/userDataStream", p, creds);
            log.info("<< keepAlive listenKey={}", listenKey);
        } catch (Exception e) {
            log.warn("[LISTENKEY_KEEPALIVE_ERR] {}", e.getMessage());
        }
    }

    /** Удаление listenKey: DELETE /api/v3/userDataStream?listenKey=... (SIGNED, без тела). */
    private void deleteListenKey(String listenKey, Creds creds) {
        try {
            Map<String, String> p = new LinkedHashMap<>();
            p.put("listenKey", listenKey);
            signedUserData("DELETE", "/api/v3/userDataStream", p, creds);
            log.info("<< delete listenKey={}", listenKey);
        } catch (Exception ignored) {}
    }

    /**
     * Локальный узкоспециализированный подписанный запрос ТОЛЬКО для userDataStream.
     * - Добавляет timestamp/recvWindow.
     * - Считает HMAC-SHA256(signature) от канонической query.
     * - Кладёт всё в URL, тело НЕ отправляется.
     */
    private JsonNode signedUserData(String method, String path, Map<String, String> params, Creds creds) {
        Objects.requireNonNull(creds, "creds is null");
        if (params == null) params = new LinkedHashMap<>();
        // 1) timestamp/recvWindow
        long ts = nowWithOffset();
        params.put("timestamp", String.valueOf(ts));
        long rw = Optional.ofNullable(props.getMexc()).map(AppProperties.Mexc::getRecvWindowMs).orElse(5_000L);
        if (rw > 0) params.put("recvWindow", String.valueOf(rw));

        // 2) каноническая строка и подпись
        String canonical = toQueryString(params);
        String sig = hmacSha256Hex(canonical, creds.getSecret());

        // 3) строим URI с query+signature
        String finalQuery = canonical + "&signature=" + sig;
        URI uri = UriComponentsBuilder.fromHttpUrl(apiBase() + path)
                .query(finalQuery)
                .build(true) // не переэнкодить повторно
                .toUri();

        // 4) заголовки (без тела)
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-MEXC-APIKEY", creds.getApiKey());
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpMethod httpMethod = switch (method.toUpperCase()) {
            case "GET"    -> HttpMethod.GET;
            case "PUT"    -> HttpMethod.PUT;
            case "DELETE" -> HttpMethod.DELETE;
            default       -> HttpMethod.POST;
        };

        try {
            log.info("{} {}?{}", httpMethod.name(), apiBase() + path, canonical + "&signature=***");
            ResponseEntity<String> resp = rest.exchange(new RequestEntity<Void>(headers, httpMethod, uri), String.class);
            String body = resp.getBody();
            return (body == null || body.isBlank()) ? om.createObjectNode() : om.readTree(body);
        } catch (RestClientException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("signedUserData error: " + e.getMessage(), e);
        }
    }

    /* =============================== WS клиент =============================== */

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

    /* =============================== Утилиты =============================== */

    private static String hmacSha256Hex(String data, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] raw = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("HMAC error: " + e.getMessage(), e);
        }
    }

    /** Собираем query-string в предсказуемом порядке (как в params). */
    private static String toQueryString(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : params.entrySet()) {
            if (sb.length() > 0) sb.append('&');
            sb.append(encode(e.getKey())).append('=').append(encode(e.getValue()));
        }
        return sb.toString();
    }

    private static String encode(String s) {
        // URL-кодирование в UTF-8 без переэнкодинга пробелов
        return UriComponentsBuilder.newInstance().queryParam("x", s).build().toUri().getQuery().substring(2);
    }

    private static <T> CompletableFuture<T> failed(String msg) {
        var f = new CompletableFuture<T>();
        f.completeExceptionally(new IllegalStateException(msg));
        return f;
    }

}
