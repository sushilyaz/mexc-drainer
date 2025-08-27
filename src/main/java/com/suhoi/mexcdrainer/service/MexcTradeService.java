package com.suhoi.mexcdrainer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MexcTradeService {

    private static final String API_BASE = "https://api.mexc.com";
    private static final String API_PREFIX = "/api/v3";

    private static final String ACCOUNT_ENDPOINT = API_PREFIX + "/account";
    private static final String ORDER_ENDPOINT = API_PREFIX + "/order";
    private static final String TICKER_BOOK = API_PREFIX + "/ticker/bookTicker";
    private static final String TIME_ENDPOINT = API_PREFIX + "/time";

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    // ======= time & signing helpers =======

    public long getServerTime() {
        try {
            String url = API_BASE + TIME_ENDPOINT;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode j = objectMapper.readTree(body);
            return j.get("serverTime").asLong();
        } catch (Exception e) {
            log.warn("Не удалось получить serverTime: {}", e.getMessage());
            return System.currentTimeMillis();
        }
    }

    private String hmacSha256Hex(String data, String secret) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] h = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(h.length * 2);
        for (byte b : h) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private String buildCanonical(Map<String, String> params) {
        return params.entrySet().stream()
                .map(e -> e.getKey() + "=" + (e.getValue() == null ? "" : e.getValue()))
                .collect(Collectors.joining("&"));
    }

    /**
     * Универсальный подписанный запрос.
     * Все POST-запросы используют query string + пустое тело.
     */
    private JsonNode signedRequest(String method, String path, Map<String, String> params, String apiKey, String secret) {
        try {
            if (params == null) params = new LinkedHashMap<>();
            params.put("timestamp", String.valueOf(getServerTime()));
            params.put("recvWindow", "5000");

            // подпись HMAC-SHA256 по параметрам
            String canonical = buildCanonical(params);
            String signature = hmacSha256Hex(canonical, secret);
            params.put("signature", signature);

            // финальный query string
            String finalQuery = buildCanonical(params);
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-MEXC-APIKEY", apiKey);
            headers.setContentType(MediaType.APPLICATION_JSON); // тело пустое, MEXC принимает

            log.info("POST https://api.mexc.com{}?{}", path, finalQuery);
            log.info("Headers: X-MEXC-APIKEY={}", apiKey);
            HttpEntity<Void> entity = new HttpEntity<>(headers);
            ResponseEntity<String> resp;

            if ("POST".equalsIgnoreCase(method)) {
                resp = restTemplate.exchange(
                        API_BASE + path + "?" + finalQuery,
                        HttpMethod.POST,
                        entity,
                        String.class
                );
            } else {
                HttpMethod httpMethod = "DELETE".equalsIgnoreCase(method) ? HttpMethod.DELETE : HttpMethod.GET;
                resp = restTemplate.exchange(
                        API_BASE + path + "?" + finalQuery,
                        httpMethod,
                        entity,
                        String.class
                );
            }

            log.debug("{} {} -> Response: {}", method, path, resp.getBody());
            return objectMapper.readTree(resp.getBody());

        } catch (org.springframework.web.client.HttpClientErrorException e) {
            String body = e.getResponseBodyAsString();
            log.error("HTTP {} {} -> status={}, body={}", method, path, e.getStatusCode(), body);
            throw new RuntimeException("Signed request error: " + e.getStatusCode() + " - " + body, e);
        } catch (Exception e) {
            throw new RuntimeException("Signed request error: " + e.getMessage(), e);
        }
    }

    // ======= BALANCES / HELPERS =======

    public BigDecimal getUsdtBalanceAccountA(Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");
        return getAssetBalance(creds.getApiKey(), creds.getSecret(), "USDT");
    }

    public BigDecimal getTokenBalanceAccountA(String symbol, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");
        String asset = symbol.endsWith("USDT") ? symbol.substring(0, symbol.length() - 4) : symbol;
        return getAssetBalance(creds.getApiKey(), creds.getSecret(), asset);
    }

    private BigDecimal getAssetBalance(String apiKey, String secretKey, String asset) {
        Map<String, String> params = new LinkedHashMap<>();
        JsonNode resp = signedRequest("GET", ACCOUNT_ENDPOINT, params, apiKey, secretKey);

        if (resp == null || !resp.has("balances")) return BigDecimal.ZERO;
        for (JsonNode b : resp.get("balances")) {
            if (b.has("asset") && asset.equalsIgnoreCase(b.get("asset").asText())) {
                String free = b.path("free").asText("0");
                try {
                    return new BigDecimal(free);
                } catch (NumberFormatException ex) {
                    return BigDecimal.ZERO;
                }
            }
        }
        return BigDecimal.ZERO;
    }

    public boolean checkBalances(BigDecimal usdtAmount, Long chatId) {
        try {
            BigDecimal usdt = getUsdtBalanceAccountA(chatId);
            log.info("Баланс A: {} USDT", usdt);
            return usdt.compareTo(usdtAmount) >= 0;
        } catch (Exception e) {
            log.error("Ошибка при checkBalances: {}", e.getMessage(), e);
            return false;
        }
    }

    // ======= ORDERS =======

    public String marketBuyAccountA(String symbol, BigDecimal usdtAmount, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "MARKET");
        params.put("quoteOrderQty", usdtAmount.toPlainString());

        JsonNode resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        if (resp != null && resp.has("orderId")) return resp.get("orderId").asText();
        log.warn("marketBuyAccountA unexpected response: {}", resp);
        return null;
    }

    public String placeLimitSellAccountA(String symbol, BigDecimal price, BigDecimal qty, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", qty.setScale(18, RoundingMode.DOWN).toPlainString());
        params.put("price", price.stripTrailingZeros().toPlainString());

        JsonNode resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        if (resp != null && resp.has("orderId")) return resp.get("orderId").asText();
        log.warn("placeLimitSellAccountA unexpected response: {}", resp);
        return null;
    }

    public String placeLimitBuyAccountA(String symbol, BigDecimal price, BigDecimal usdtAmount, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        BigDecimal qty = BigDecimal.ZERO;
        try {
            qty = usdtAmount.divide(price, 8, RoundingMode.DOWN);
        } catch (ArithmeticException ex) {
            qty = BigDecimal.ZERO;
        }
        if (qty.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("placeLimitBuyAccountA: рассчитанное quantity <= 0 (usdt={}, price={})", usdtAmount, price);
            return null;
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", qty.setScale(18, RoundingMode.DOWN).toPlainString());
        params.put("price", price.stripTrailingZeros().toPlainString());

        JsonNode resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        if (resp != null && resp.has("orderId")) return resp.get("orderId").asText();
        log.warn("placeLimitBuyAccountA unexpected response: {}", resp);
        return null;
    }

    private void placeLimitOrder(String symbol, String side, BigDecimal price, BigDecimal qty, String apiKey, String secret) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", side);
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", qty.setScale(18, RoundingMode.DOWN).toPlainString());
        params.put("price", price.stripTrailingZeros().toPlainString());

        signedRequest("POST", ORDER_ENDPOINT, params, apiKey, secret);
    }

    public void buyFromAccountB(String symbol, BigDecimal price, BigDecimal qty, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");
        placeLimitOrder(symbol, "BUY", price, qty, creds.getApiKey(), creds.getSecret());
    }

    public void sellFromAccountB(String symbol, BigDecimal price, BigDecimal usdtAmount, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        BigDecimal qty = BigDecimal.ZERO;
        try {
            qty = usdtAmount.divide(price, 8, RoundingMode.DOWN);
        } catch (ArithmeticException ex) {
            qty = BigDecimal.ZERO;
        }
        if (qty.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("sellFromAccountB: calculated qty <= 0 (usdt={}, price={})", usdtAmount, price);
            return;
        }
        placeLimitOrder(symbol, "SELL", price, qty, creds.getApiKey(), creds.getSecret());
    }

    public void forceMarketSellAccountA(String symbol, BigDecimal qty, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "MARKET");
        params.put("quantity", qty.setScale(18, RoundingMode.DOWN).toPlainString());

        signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
    }

    // ======= SPREAD helpers =======

    public BigDecimal getNearLowerSpreadPrice(String symbol) {
        try {
            String url = API_BASE + TICKER_BOOK + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode resp = objectMapper.readTree(body);
            BigDecimal bid = new BigDecimal(resp.get("bidPrice").asText());
            BigDecimal ask = new BigDecimal(resp.get("askPrice").asText());
            BigDecimal spread = ask.subtract(bid);
            return bid.add(spread.multiply(BigDecimal.valueOf(0.1))).setScale(18, RoundingMode.HALF_UP);
        } catch (Exception e) {
            log.error("Ошибка получения стакана: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public BigDecimal getNearUpperSpreadPrice(String symbol) {
        try {
            String url = API_BASE + TICKER_BOOK + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode resp = objectMapper.readTree(body);
            BigDecimal bid = new BigDecimal(resp.get("bidPrice").asText());
            BigDecimal ask = new BigDecimal(resp.get("askPrice").asText());
            BigDecimal spread = ask.subtract(bid);
            return ask.subtract(spread.multiply(BigDecimal.valueOf(0.1))).setScale(18, RoundingMode.HALF_UP);
        } catch (Exception e) {
            log.error("Ошибка получения стакана: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    // ======= тестовый метод можно оставить =======

    public String signedMarketBuy(String symbol, BigDecimal usdtAmount, String apiKey, String secret) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "MARKET");
        params.put("quoteOrderQty", usdtAmount.toPlainString());

        JsonNode resp = signedRequest("POST", ORDER_ENDPOINT, params, apiKey, secret);
        if (resp != null && resp.has("orderId")) {
            return resp.get("orderId").asText();
        }
        log.warn("signedMarketBuy response: {}", resp);
        return null;
    }

    private JsonNode signedRequestTest(String method, String path, Map<String,String> params, String apiKey, String secret) {
        try {
            if (params == null) params = new LinkedHashMap<>();
            params.put("timestamp", String.valueOf(getServerTime()));
            params.put("recvWindow", "5000");

            // Формируем строку параметров для подписи
            String queryString = params.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("&"));

            // Генерируем подпись
            String signature = hmacSha256Hex(queryString, secret);
            params.put("signature", signature);

            // Формируем окончательный query string
            String finalQuery = params.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("&"));

            // Логи
            System.out.println("POST " + "https://api.mexc.com" + path + "?" + finalQuery);
            System.out.println("Headers: X-MEXC-APIKEY=" + apiKey);

            HttpHeaders headers = new HttpHeaders();
            headers.set("X-MEXC-APIKEY", apiKey);
            headers.setContentType(MediaType.APPLICATION_JSON); // тело пустое, MEXC принимает

            HttpEntity<Void> entity = new HttpEntity<>(headers);
            ResponseEntity<String> resp = restTemplate.exchange(
                    "https://api.mexc.com" + path + "?" + finalQuery,
                    HttpMethod.POST,
                    entity,
                    String.class
            );

            System.out.println("Response: " + resp.getBody());
            return objectMapper.readTree(resp.getBody());
        } catch (Exception e) {
            throw new RuntimeException("Signed request error: " + e.getMessage(), e);
        }
    }

}
