package com.suhoi.mexcdrainer.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SIGNED-клиент для /api/v3/userDataStream:
 *  - POST   /api/v3/userDataStream                     -> create listenKey
 *  - PUT    /api/v3/userDataStream?listenKey=...       -> keepalive
 *  - DELETE /api/v3/userDataStream?listenKey=...       -> close
 *
 * Все вызовы:
 *  - добавляют timestamp и recvWindow;
 *  - подписывают query (HMAC-SHA256) и кладут signature в URL;
 *  - НЕ отправляют тела (body пустой);
 *  - кладут заголовок X-MEXC-APIKEY.
 *
 * Реализована фоновая синхронизация времени через /api/v3/time (с TTL).
 */
@Slf4j
@RequiredArgsConstructor
public class ListenKeyClient {

    private static final String API_BASE = "https://api.mexc.com";
    private static final String USER_STREAM = "/api/v3/userDataStream";
    private static final String TIME_ENDPOINT = "/api/v3/time";

    /** recvWindow по умолчанию, если не хочешь тянуть из AppProperties */
    private static final long DEFAULT_RECV_WINDOW_MS = 5_000L;

    /** Период актуальности смещения времени (5 минут). */
    private static final long TIME_SYNC_TTL_MS = 5 * 60_000L;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    // --- коррекция времени сервера ---
    private final AtomicLong timeOffsetMs = new AtomicLong(0L);
    private final AtomicLong timeSyncedAtMs = new AtomicLong(0L);

    /* =========================== Публичное API =========================== */

    /** Создание listenKey: SIGNED POST без тела. */
    public String createListenKey(String apiKey, String secret) {
        JsonNode r = signedCall("POST", USER_STREAM, new LinkedHashMap<>(), apiKey, secret);
        String key = r.path("listenKey").asText(null);
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("listenKey пуст в ответе: " + safeJson(r));
        }
        log.info("listenKey created: {}", key);
        return key;
    }

    /** Продление listenKey: SIGNED PUT без тела. */
    public void keepAlive(String apiKey, String secret, String listenKey) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("listenKey", listenKey);
        JsonNode r = signedCall("PUT", USER_STREAM, p, apiKey, secret);
        log.debug("listenKey keepalive ok: {}", safeJson(r));
    }

    /** Закрытие listenKey: SIGNED DELETE без тела. */
    public void close(String apiKey, String secret, String listenKey) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("listenKey", listenKey);
        JsonNode r = signedCall("DELETE", USER_STREAM, p, apiKey, secret);
        log.info("listenKey closed: {}", safeJson(r));
    }

    /* =========================== Внутренности =========================== */

    /**
     * Универсальный SIGNED-вызов для userDataStream:
     * - добавляет timestamp/recvWindow,
     * - подписывает каноническую строку (в ПОРЯДКЕ ДОБАВЛЕНИЯ),
     * - кладёт всё в URL,
     * - ставит заголовок X-MEXC-APIKEY,
     * - возвращает JSON.
     */
    private JsonNode signedCall(String method,
                                String path,
                                Map<String, String> params,
                                String apiKey,
                                String secret) {
        Objects.requireNonNull(apiKey, "apiKey");
        Objects.requireNonNull(secret, "secret");

        try {
            if (params == null) params = new LinkedHashMap<>();

            // 1) timestamp / recvWindow (используем коррекцию времени сервера)
            long ts = nowWithOffset();
            params.put("timestamp", String.valueOf(ts));
            params.put("recvWindow", String.valueOf(DEFAULT_RECV_WINDOW_MS));

            // 2) каноническая строка (в порядке добавления) и подпись
            String canonical = toQueryString(params);
            String signature = hmacSha256Hex(canonical, secret);
            String finalQuery = canonical + "&signature=" + signature;

            // 3) строим URI, кладём заголовки (без тела)
            URI uri = UriComponentsBuilder
                    .fromHttpUrl(API_BASE + path)
                    .query(finalQuery) // уже закодировано — НЕ переэнкодим
                    .build(true).toUri();

            HttpHeaders headers = new HttpHeaders();
            headers.set("X-MEXC-APIKEY", apiKey);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpMethod httpMethod = switch (method.toUpperCase()) {
                case "GET"    -> HttpMethod.GET;
                case "PUT"    -> HttpMethod.PUT;
                case "DELETE" -> HttpMethod.DELETE;
                default       -> HttpMethod.POST;
            };

            log.info("{} {}?{}",
                    httpMethod.name(),
                    API_BASE + path,
                    canonical + "&signature=***");

            ResponseEntity<String> resp =
                    restTemplate.exchange(new RequestEntity<Void>(headers, httpMethod, uri), String.class);

            String body = resp.getBody();
            return (body == null || body.isBlank())
                    ? objectMapper.createObjectNode()
                    : objectMapper.readTree(body);

        } catch (RestClientException e) {
            // Пробрасываем как есть — полезно видеть HTTP-коды/ответы
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Signed userDataStream request error: " + e.getMessage(), e);
        }
    }

    /** Возвращает «текущее» время с учётом смещения сервера. Раз в TIME_SYNC_TTL_MS обновляем offset. */
    private long nowWithOffset() {
        long now = System.currentTimeMillis();
        if (now - timeSyncedAtMs.get() > TIME_SYNC_TTL_MS) {
            syncServerTimeQuiet();
        }
        return now + timeOffsetMs.get();
    }

    /** Тихо подтягиваем /api/v3/time и корректируем offset. Ошибку не считаем фатальной. */
    private void syncServerTimeQuiet() {
        try {
            URI uri = UriComponentsBuilder.fromHttpUrl(API_BASE + TIME_ENDPOINT).build(true).toUri();
            ResponseEntity<String> resp = restTemplate.exchange(
                    new RequestEntity<Void>(new HttpHeaders(), HttpMethod.GET, uri), String.class);
            JsonNode j = objectMapper.readTree(resp.getBody());
            long serverTime = j.path("serverTime").asLong(0L);
            long localNow = System.currentTimeMillis();
            long offset = serverTime - localNow;
            timeOffsetMs.set(offset);
            timeSyncedAtMs.set(localNow);
            log.info("[TIME_SYNC] serverTime={} localNow={} offsetMs={}", serverTime, localNow, offset);
        } catch (Exception e) {
            // не фатально — продолжаем с локальным временем
            log.debug("[TIME_SYNC_ERR] {}", e.getMessage());
            timeSyncedAtMs.set(System.currentTimeMillis());
        }
    }

    /** Собираем query-string в порядке добавления параметров (как в твоём рабочем коде). */
    private static String toQueryString(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : params.entrySet()) {
            if (sb.length() > 0) sb.append('&');
            sb.append(encode(e.getKey())).append('=').append(encode(e.getValue()));
        }
        return sb.toString();
    }

    /** URL-encode без двойного кодирования (через UriComponentsBuilder). */
    private static String encode(String s) {
        return UriComponentsBuilder.newInstance().queryParam("x", s).build().toUri().getQuery().substring(2);
    }

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

    private String safeJson(JsonNode n) {
        try { return (n == null) ? "null" : objectMapper.writeValueAsString(n); }
        catch (Exception ignore) { return String.valueOf(n); }
    }
}

