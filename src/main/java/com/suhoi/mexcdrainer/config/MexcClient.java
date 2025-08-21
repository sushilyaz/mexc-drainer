package com.suhoi.mexcdrainer.config;

import com.suhoi.mexcdrainer.dto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class MexcClient {
    private final RestTemplate http;
    private final AppProperties props;

    private String base() { return props.getMexc().getBaseUrl(); }
    private long recvWindow() { return props.getMexc().getRecvWindowMs(); }

    // ----------------------- public (неподписанные)
    public ExchangeInfoResponse exchangeInfo(String symbol) {
        String url = base() + "/api/v3/exchangeInfo";
        if (symbol != null) url += "?symbol=" + symbol;
        return http.getForObject(url, ExchangeInfoResponse.class);
    }

    public BookTicker bookTicker(String symbol) {
        String url = base() + "/api/v3/ticker/bookTicker?symbol=" + symbol;
        return http.getForObject(url, BookTicker.class);
    }

    public Depth depth(String symbol, int limit) {
        String url = base() + "/api/v3/depth?symbol=" + symbol + "&limit=" + limit;
        return http.getForObject(url, Depth.class);
    }

    // ----------------------- signed helpers (QS + пустое тело)
    private static String canonicalQS(Map<String, String> params) {
        // без URL-энкода — как в твоём рабочем примере
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (var e : params.entrySet()) {
            if (!first) sb.append('&');
            first = false;
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }

    private static String sign(String secretKey, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] out = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(out.length * 2);
            for (byte b : out) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (Exception e) {
            throw new IllegalStateException("HMAC error", e);
        }
    }

    private LinkedHashMap<String, String> signedParams(String secret, Map<String, String> in) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        // сначала твои параметры
        if (in != null) map.putAll(in);
        // затем служебные
        map.put("recvWindow", String.valueOf(recvWindow()));
        map.put("timestamp", String.valueOf(Instant.now().toEpochMilli()));
        // подпись по канонической строке (без URL-энкода)
        String qs = canonicalQS(map);
        map.put("signature", sign(secret, qs));
        return map;
    }

    private HttpHeaders headersGet(String apiKey) {
        HttpHeaders h = new HttpHeaders();
        h.set("X-MEXC-APIKEY", apiKey);
        h.setAccept(List.of(MediaType.APPLICATION_JSON));
        return h;
    }

    private HttpHeaders headersPostDelete(String apiKey) {
        HttpHeaders h = new HttpHeaders();
        h.set("X-MEXC-APIKEY", apiKey);
        // КРИТИЧНО: JSON + пустое тело
        h.setContentType(MediaType.APPLICATION_JSON);
        h.setAccept(List.of(MediaType.APPLICATION_JSON));
        return h;
    }

    // ----------------------- signed endpoints (spot)
    public AccountInfo account(String apiKey, String secret) {
        LinkedHashMap<String,String> p = signedParams(secret, Map.of());
        String url = base() + "/api/v3/account" + "?" + canonicalQS(p);
        ResponseEntity<AccountInfo> resp = http.exchange(url, HttpMethod.GET, new HttpEntity<>(headersGet(apiKey)), AccountInfo.class);
        return resp.getBody();
    }

    public List<OpenOrder> openOrders(String apiKey, String secret, String symbol) {
        LinkedHashMap<String,String> in = new LinkedHashMap<>();
        in.put("symbol", symbol);
        LinkedHashMap<String,String> p = signedParams(secret, in);
        String url = base() + "/api/v3/openOrders" + "?" + canonicalQS(p);
        ResponseEntity<OpenOrder[]> resp = http.exchange(url, HttpMethod.GET, new HttpEntity<>(headersGet(apiKey)), OpenOrder[].class);
        OpenOrder[] arr = resp.getBody();
        return Arrays.asList(arr == null ? new OpenOrder[0] : arr);
    }

    public OrderResponse newOrder(String apiKey, String secret, NewOrderRequest req) {
        LinkedHashMap<String,String> in = new LinkedHashMap<>();
        in.put("symbol", req.getSymbol());
        in.put("side", req.getSide());
        in.put("type", req.getType());
        if (req.getQuantity() != null)        in.put("quantity", req.getQuantity());
        if (req.getQuoteOrderQty() != null)   in.put("quoteOrderQty", req.getQuoteOrderQty());
        if (req.getPrice() != null)           in.put("price", req.getPrice());
        if (req.getNewClientOrderId() != null)in.put("newClientOrderId", req.getNewClientOrderId());

        LinkedHashMap<String,String> p = signedParams(secret, in);
        String url = base() + "/api/v3/order" + "?" + canonicalQS(p);

        try {
            // тело пустое, заголовки JSON
            ResponseEntity<OrderResponse> resp =
                    http.exchange(url, HttpMethod.POST, new HttpEntity<>(headersPostDelete(apiKey)), OrderResponse.class);
            return resp.getBody();
        } catch (HttpStatusCodeException e) {
            log.error("POST /api/v3/order failed: {}", e.getResponseBodyAsString());
            throw e;
        }
    }

    public void cancelOrder(String apiKey, String secret, String symbol, String orderId) {
        java.util.LinkedHashMap<String,String> in = new java.util.LinkedHashMap<>();
        in.put("symbol", symbol);
        in.put("orderId", String.valueOf(orderId)); // уже строка, но не мешает
        java.util.LinkedHashMap<String,String> p = signedParams(secret, in);
        String url = base() + "/api/v3/order" + "?" + canonicalQS(p);
        http.exchange(url, org.springframework.http.HttpMethod.DELETE,
                new org.springframework.http.HttpEntity<>(headersPostDelete(apiKey)), String.class);
    }
}
