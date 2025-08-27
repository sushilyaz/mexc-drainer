package com.suhoi.mexcdrainer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

/**
 * Сервис работы с MEXC API (торговля, балансы, стакан).
 */
@Service
@Slf4j
public class MexcTradeService {

    private final String API_URL = "https://api.mexc.com";

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();


    // оффсет времени между локальным и серверным
    private volatile long timeOffset = 0L;

    public MexcTradeService() {
        syncServerTime();
        // Можно периодически обновлять оффсет в отдельном scheduler-е
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::syncServerTime, 0, 30, TimeUnit.SECONDS);
    }

    private void syncServerTime() {
        try {
            ResponseEntity<Map> resp = restTemplate.getForEntity(API_URL + "/api/v3/time", Map.class);
            if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                long serverTime = ((Number) resp.getBody().get("serverTime")).longValue();
                long localTime = System.currentTimeMillis();
                timeOffset = serverTime - localTime;
                System.out.println("[SYNC] Серверное время: " + serverTime + ", локальное: " + localTime + ", оффсет=" + timeOffset);
            }
        } catch (Exception e) {
            System.err.println("[SYNC] Ошибка получения времени: " + e.getMessage());
        }
    }

    private long currentTimestamp() {
        return System.currentTimeMillis() + timeOffset;
    }

    // ===== Вспомогательные методы =====

    private String sign(String query, String secretKey) throws Exception {
        Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
        SecretKeySpec secret_key = new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        sha256_HMAC.init(secret_key);
        return HexFormat.of().formatHex(sha256_HMAC.doFinal(query.getBytes(StandardCharsets.UTF_8)));
    }

    private String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1)
                hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private ResponseEntity<String> signedRequest(String method, String path, Map<String, String> params, String apiKey, String secretKey) throws Exception {
        params.put("timestamp", String.valueOf(currentTimestamp()));

        StringBuilder query = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (query.length() > 0) query.append("&");
            query.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8))
                    .append("=")
                    .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
        }

        String signature = sign(query.toString(), secretKey);
        query.append("&signature=").append(signature);

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-MEXC-APIKEY", apiKey);

        HttpEntity<String> entity = new HttpEntity<>(headers);
        String url = API_URL + path + "?" + query;

        if (method.equalsIgnoreCase("GET")) {
            return restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
        } else if (method.equalsIgnoreCase("POST")) {
            return restTemplate.exchange(url, HttpMethod.POST, entity, String.class);
        } else if (method.equalsIgnoreCase("DELETE")) {
            return restTemplate.exchange(url, HttpMethod.DELETE, entity, String.class);
        }
        throw new IllegalArgumentException("Unsupported method: " + method);
    }

    // ===== Основные методы =====

    /**
     * Проверка балансов
     */
    public boolean checkBalances(BigDecimal usdtAmount, Long chatId) {
        try {
            BigDecimal balanceA = getUsdtBalance(MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret());
            BigDecimal balanceB = getUsdtBalance(MemoryDb.getAccountB(chatId).getApiKey(), MemoryDb.getAccountB(chatId).getSecret());

            log.info("💰 Баланс A: {} USDT | Баланс B: {} USDT", balanceA, balanceB);

            return balanceA.compareTo(usdtAmount) >= 0 && balanceB.compareTo(BigDecimal.ONE) >= 0;
        } catch (Exception e) {
            log.error("Ошибка при проверке балансов", e);
            return false;
        }
    }

    private BigDecimal getUsdtBalance(String apiKey, String secretKey) throws Exception {
        ResponseEntity<String> resp = signedRequest("GET", "/api/v3/account", new HashMap<>(), apiKey, secretKey);
        JsonNode root = objectMapper.readTree(resp.getBody());
        for (JsonNode asset : root.get("balances")) {
            if (asset.get("asset").asText().equals("USDT")) {
                return new BigDecimal(asset.get("free").asText());
            }
        }
        return BigDecimal.ZERO;
    }

    /**
     * Рыночная покупка аккаунтом A
     */
    public String marketBuyAccountA(String symbol, BigDecimal usdtAmount, Long chatId) throws Exception {
        return placeOrder(MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret(), symbol, "BUY", "MARKET", usdtAmount, null);
    }

    /**
     * Лимитная продажа аккаунтом A
     */
    public String placeLimitSellAccountA(String symbol, BigDecimal price, BigDecimal usdtAmount, Long chatId) throws Exception {
        return placeOrder(MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret(), symbol, "SELL", "LIMIT", usdtAmount, price);
    }

    /**
     * Лимитная покупка аккаунтом A
     */
    public String placeLimitBuyAccountA(String symbol, BigDecimal price, BigDecimal usdtAmount, Long chatId) throws Exception {
        return placeOrder(MemoryDb.getAccountA(chatId).getApiKey(), MemoryDb.getAccountA(chatId).getSecret(), symbol, "BUY", "LIMIT", usdtAmount, price);
    }

    /**
     * Аккаунт B покупает у A
     */
    public void buyFromAccountB(String symbol, BigDecimal price, BigDecimal usdtAmount,  Long chatId) throws Exception {
        placeOrder(MemoryDb.getAccountB(chatId).getApiKey(), MemoryDb.getAccountB(chatId).getSecret(), symbol, "BUY", "MARKET", usdtAmount, null);
    }

    /**
     * Аккаунт B продаёт
     */
    public void sellFromAccountB(String symbol, BigDecimal price, BigDecimal usdtAmount, Long chatId) throws Exception {
        placeOrder(MemoryDb.getAccountB(chatId).getApiKey(), MemoryDb.getAccountB(chatId).getSecret(), symbol, "SELL", "MARKET", usdtAmount, null);
    }

    /**
     * Форсированная продажа аккаунтом A
     */
    public void forceMarketSellAccountA(String symbol, BigDecimal usdtAmount, Long chatId) throws Exception {
        try {
            placeOrder(MemoryDb.getAccountB(chatId).getApiKey(), MemoryDb.getAccountB(chatId).getSecret(), symbol, "SELL", "MARKET", usdtAmount, null);
        } catch (Exception e) {
            log.error("Ошибка при аварийной продаже", e);
        }
    }

    /**
     * Создание ордера
     */
    private String placeOrder(String apiKey, String secretKey, String symbol, String side,
                              String type, BigDecimal usdtAmount, BigDecimal price) throws Exception {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", side);
        params.put("type", type);

        if (type.equals("MARKET")) {
            // Для MARKET на MEXC нужно указывать quoteOrderQty (в USDT)
            params.put("quoteOrderQty", usdtAmount.toPlainString());
        } else if (type.equals("LIMIT")) {
            params.put("price", price.toPlainString());
            // Считаем количество монеты
            BigDecimal qty = usdtAmount.divide(price, 8, BigDecimal.ROUND_DOWN);
            params.put("quantity", qty.toPlainString());
            params.put("timeInForce", "GTC");
        }

        ResponseEntity<String> resp = signedRequest("POST", "/api/v3/order", params, apiKey, secretKey);
        JsonNode root = objectMapper.readTree(resp.getBody());

        String orderId = root.get("orderId").asText();
        log.info("📑 Ордер создан: {} {} {} {} (id={})", side, type, symbol, usdtAmount, orderId);

        return orderId;
    }

    /**
     * Получение цены возле нижней границы спреда
     */
    public BigDecimal getNearLowerSpreadPrice(String symbol) {
        try {
            String url = API_URL + "/api/v3/depth?symbol=" + symbol + "&limit=5";
            String body = restTemplate.getForObject(url, String.class);
            JsonNode root = objectMapper.readTree(body);

            BigDecimal bestBid = new BigDecimal(root.get("bids").get(0).get(0).asText());
            BigDecimal bestAsk = new BigDecimal(root.get("asks").get(0).get(0).asText());

            BigDecimal price = bestBid.add(bestAsk.subtract(bestBid).multiply(BigDecimal.valueOf(0.1))); // 10% от спреда внутрь
            return price.setScale(6, BigDecimal.ROUND_HALF_UP);
        } catch (Exception e) {
            log.error("Ошибка получения стакана", e);
            return BigDecimal.ONE;
        }
    }

    /**
     * Получение цены возле верхней границы спреда
     */
    public BigDecimal getNearUpperSpreadPrice(String symbol) {
        try {
            String url = API_URL + "/api/v3/depth?symbol=" + symbol + "&limit=5";
            String body = restTemplate.getForObject(url, String.class);
            JsonNode root = objectMapper.readTree(body);

            BigDecimal bestBid = new BigDecimal(root.get("bids").get(0).get(0).asText());
            BigDecimal bestAsk = new BigDecimal(root.get("asks").get(0).get(0).asText());

            BigDecimal price = bestAsk.subtract(bestAsk.subtract(bestBid).multiply(BigDecimal.valueOf(0.1))); // 10% внутрь
            return price.setScale(6, BigDecimal.ROUND_HALF_UP);
        } catch (Exception e) {
            log.error("Ошибка получения стакана", e);
            return BigDecimal.ONE;
        }
    }
}
