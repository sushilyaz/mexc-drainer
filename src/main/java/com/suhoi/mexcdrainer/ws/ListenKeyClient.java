package com.suhoi.mexcdrainer.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class ListenKeyClient {

    private static final String API_BASE = "https://api.mexc.com";
    private static final String USER_STREAM = "/api/v3/userDataStream";
    private static final long DEFAULT_RECV_WINDOW_MS = 5_000L;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public String createListenKey(String apiKey, String secret) {
        JsonNode r = signed("POST", new LinkedHashMap<>(), apiKey, secret);
        String key = r.path("listenKey").asText(null);
        if (key == null || key.isBlank()) throw new IllegalStateException("listenKey пуст в ответе: " + r);
        log.info("🔐 listenKey created: {}", key);
        return key;
    }

    public void keepAlive(String apiKey, String secret, String listenKey) {
        var p = new LinkedHashMap<String,String>(); p.put("listenKey", listenKey);
        signed("PUT", p, apiKey, secret);
    }

    public void close(String apiKey, String secret, String listenKey) {
        var p = new LinkedHashMap<String,String>(); p.put("listenKey", listenKey);
        signed("DELETE", p, apiKey, secret);
    }

    private JsonNode signed(String method, Map<String,String> params, String apiKey, String secret) {
        try {
            long ts = System.currentTimeMillis();
            params.put("timestamp", String.valueOf(ts));
            params.put("recvWindow", String.valueOf(DEFAULT_RECV_WINDOW_MS));
            String canonical = toQuery(params);
            String signature = hmac(canonical, secret);
            String finalQuery = canonical + "&signature=" + signature;

            HttpMethod http = switch (method) {
                case "GET" -> HttpMethod.GET;
                case "PUT" -> HttpMethod.PUT;
                case "DELETE" -> HttpMethod.DELETE;
                default -> HttpMethod.POST;
            };

            URI uri = UriComponentsBuilder.fromHttpUrl(API_BASE + USER_STREAM).query(finalQuery).build(true).toUri();
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-MEXC-APIKEY", apiKey);
            var resp = restTemplate.exchange(new RequestEntity<Void>(headers, http, uri), String.class);
            String body = resp.getBody();
            return (body == null || body.isBlank()) ? objectMapper.createObjectNode() : objectMapper.readTree(body);
        } catch (Exception e) {
            throw new RuntimeException("listenKey signed call failed: " + e.getMessage(), e);
        }
    }

    private static String toQuery(Map<String,String> p) {
        StringBuilder sb = new StringBuilder();
        for (var e : p.entrySet()) {
            if (sb.length() > 0) sb.append('&');
            sb.append(encode(e.getKey())).append('=').append(encode(e.getValue()));
        }
        return sb.toString();
    }
    private static String encode(String s) {
        return UriComponentsBuilder.newInstance().queryParam("x", s).build().toUri().getQuery().substring(2);
    }
    private static String hmac(String data, String secret) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] raw = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(raw.length * 2);
        for (byte b : raw) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
