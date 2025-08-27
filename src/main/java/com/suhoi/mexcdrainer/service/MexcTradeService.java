package com.suhoi.mexcdrainer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class MexcTradeService {

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${mexc.api.baseurl:https://api.mexc.com}")

    private static final String ACCOUNT_ENDPOINT = "/api/v3/account";
    private static final String ORDER_ENDPOINT = "/api/v3/order";
    private static final String EXCHANGE_INFO = "/api/v3/exchangeInfo";
    private static final String TICKER_BOOK = "/api/v3/ticker/bookTicker";

    /* =========================== SIGNED REQUEST =========================== */

    private String signParams(String query, String secretKey) throws Exception {
        Mac hmacSHA256 = Mac.getInstance("HmacSHA256");
        SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        hmacSHA256.init(secretKeySpec);
        byte[] hash = hmacSHA256.doFinal(query.getBytes(StandardCharsets.UTF_8));
        StringBuilder result = new StringBuilder();
        for (byte b : hash) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    private JsonNode signedRequest(String url, Map<String, String> params, String apiKey, String secretKey, HttpMethod method) {
        try {
            params.put("timestamp", String.valueOf(System.currentTimeMillis()));

            String query = getQuery(params);
            String signature = signParams(query, secretKey);
            query += "&signature=" + signature;

            HttpHeaders headers = new HttpHeaders();
            headers.set("X-MEXC-APIKEY", apiKey);
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            HttpEntity<?> entity = new HttpEntity<>(headers);

            ResponseEntity<String> resp = restTemplate.exchange(url + "?" + query, method, entity, String.class);
            return objectMapper.readTree(resp.getBody());
        } catch (Exception e) {
            throw new RuntimeException("Signed request error: " + e.getMessage(), e);
        }
    }

    private String getQuery(Map<String, String> params) {
        return params.entrySet().stream()
                .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .reduce((a, b) -> a + "&" + b).orElse("");
    }

    /* =========================== BALANCES =========================== */

    public BigDecimal getUsdtBalanceAccountA(Long chatId) {
        return getAssetBalance(MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret(), "USDT");
    }

    public BigDecimal getTokenBalanceAccountA(String symbol, Long chatId) {
        String asset = symbol.replace("USDT", ""); // например ANTUSDT → ANT
        return getAssetBalance(MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret(), asset);
    }

    public BigDecimal getAssetBalance(String apiKey, String secretKey, String asset) {
        Map<String, String> params = new HashMap<>();
        JsonNode resp = signedRequest("https://api.mexc.com" + ACCOUNT_ENDPOINT, params, apiKey, secretKey, HttpMethod.GET);

        for (JsonNode balance : resp.get("balances")) {
            if (balance.get("asset").asText().equalsIgnoreCase(asset)) {
                return new BigDecimal(balance.get("free").asText());
            }
        }
        return BigDecimal.ZERO;
    }

    public boolean checkBalances(BigDecimal usdtAmount, Long chatId) {
        BigDecimal usdtA = getUsdtBalanceAccountA(chatId);
        log.info("Баланс A: {} USDT", usdtA);
        return usdtA.compareTo(usdtAmount) >= 0;
    }

    /* =========================== ORDERS =========================== */

    public String marketBuyAccountA(String symbol, BigDecimal usdtAmount, Long chatId) {
        Map<String, String> params = new HashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "MARKET");
        params.put("quoteOrderQty", usdtAmount.toPlainString()); // покупка на сумму USDT

        JsonNode resp = signedRequest("https://api.mexc.com" + ORDER_ENDPOINT, params, MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret(), HttpMethod.POST);
        return resp.get("orderId").asText();
    }

    public String placeLimitSellAccountA(String symbol, BigDecimal price, BigDecimal qty, Long chatId) {
        Map<String, String> params = new HashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", qty.toPlainString());
        params.put("price", price.toPlainString());

        JsonNode resp = signedRequest("https://api.mexc.com" + ORDER_ENDPOINT, params, MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret(), HttpMethod.POST);
        return resp.get("orderId").asText();
    }

    public String placeLimitBuyAccountA(String symbol, BigDecimal price, BigDecimal usdtAmount, Long chatId) {
        Map<String, String> params = new HashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quoteOrderQty", usdtAmount.toPlainString());
        params.put("price", price.toPlainString());

        JsonNode resp = signedRequest("https://api.mexc.com" + ORDER_ENDPOINT, params, MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret(), HttpMethod.POST);
        return resp.get("orderId").asText();
    }

    public void buyFromAccountB(String symbol, BigDecimal price, BigDecimal qty, Long chatId) {
        placeLimitOrder(symbol, "BUY", price, qty, MemoryDb.getAccountB(chatId).getApiKey(), MemoryDb.getAccountB(chatId).getSecret());
    }

    public void sellFromAccountB(String symbol, BigDecimal price, BigDecimal usdtAmount, Long chatId) {
        BigDecimal qty = usdtAmount.divide(price, 8, BigDecimal.ROUND_DOWN);
        placeLimitOrder(symbol, "SELL", price, qty, MemoryDb.getAccountB(chatId).getApiKey(), MemoryDb.getAccountB(chatId).getSecret());
    }

    private void placeLimitOrder(String symbol, String side, BigDecimal price, BigDecimal qty,
                                 String apiKey, String secretKey) {
        Map<String, String> params = new HashMap<>();
        params.put("symbol", symbol);
        params.put("side", side);
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", qty.toPlainString());
        params.put("price", price.toPlainString());

        signedRequest("https://api.mexc.com" + ORDER_ENDPOINT, params, apiKey, secretKey, HttpMethod.POST);
    }

    public void forceMarketSellAccountA(String symbol, BigDecimal qty, Long chatId) {
        Map<String, String> params = new HashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "MARKET");
        params.put("quantity", qty.toPlainString());

        signedRequest("https://api.mexc.com" + ORDER_ENDPOINT, params, MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret(), HttpMethod.POST);
    }

    /* =========================== СПРЕД =========================== */

    public BigDecimal getNearLowerSpreadPrice(String symbol) {
        try {
            String url = "https://api.mexc.com" + TICKER_BOOK + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode resp = objectMapper.readTree(body);
            BigDecimal bid = new BigDecimal(resp.get("bidPrice").asText());
            BigDecimal ask = new BigDecimal(resp.get("askPrice").asText());
            return bid.add(ask).divide(BigDecimal.valueOf(2), 8, BigDecimal.ROUND_HALF_UP);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка при получении спреда", e);
        }
    }

    public BigDecimal getNearUpperSpreadPrice(String symbol) {
        try {
            String url = "https://api.mexc.com" + TICKER_BOOK + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode resp = objectMapper.readTree(body);
            BigDecimal bid = new BigDecimal(resp.get("bidPrice").asText());
            BigDecimal ask = new BigDecimal(resp.get("askPrice").asText());
            return ask.subtract(ask.subtract(bid).divide(BigDecimal.valueOf(2), 8, BigDecimal.ROUND_HALF_UP));
        } catch (Exception e) {
            throw new RuntimeException("Ошибка при получении спреда", e);
        }
    }
}
