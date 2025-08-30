package com.suhoi.mexcdrainer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final String EXCHANGE_INFO_ENDPOINT = "/api/v3/exchangeInfo";

    public static final long EXCHANGE_INFO_TTL_MS = 60_000L;

    // --- Комиссии (настраиваемые). По умолчанию 0.2% и небольшой safety-запас.
    private static final BigDecimal MAKER_FEE = new BigDecimal("0.0000"); // 0%
    private static final BigDecimal TAKER_FEE = new BigDecimal("0.0005"); // 0.05%
    private static final BigDecimal FEE_SAFETY = new BigDecimal("0.0010"); // +0.10% запас

    public final Map<String, CachedSymbolInfo> exchangeInfoCache = new ConcurrentHashMap<>();

    public static final class SymbolFilters {
        final BigDecimal tickSize;     // PRICE_FILTER.tickSize (цена)
        final BigDecimal stepSize;     // LOT_SIZE.stepSize (кол-во базовой)
        final BigDecimal minQty;       // LOT_SIZE.minQty
        final BigDecimal minNotional;  // MIN_NOTIONAL.minNotional (может быть 0 у MEXC)
        final Integer   quotePrecision;// сколько знаков разрешено у quote (USDT) для quoteOrderQty

        SymbolFilters(BigDecimal tickSize,
                      BigDecimal stepSize,
                      BigDecimal minQty,
                      BigDecimal minNotional,
                      Integer quotePrecision) {
            this.tickSize = tickSize != null ? tickSize : BigDecimal.ZERO;
            this.stepSize = stepSize != null ? stepSize : BigDecimal.ONE;
            this.minQty = minQty != null ? minQty : BigDecimal.ZERO;
            this.minNotional = minNotional != null ? minNotional : BigDecimal.ZERO;
            this.quotePrecision = quotePrecision; // может быть null
        }
    }

    public static final class CachedSymbolInfo {
        final SymbolFilters filters;
        final long loadedAt;
        CachedSymbolInfo(SymbolFilters filters, long loadedAt) {
            this.filters = filters;
            this.loadedAt = loadedAt;
        }
    }

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
     * Универсальный подписанный запрос. Для GET/DELETE/POST.
     */
    private JsonNode signedRequest(String method, String path, Map<String, String> params, String apiKey, String secret) {
        try {
            if (params == null) params = new LinkedHashMap<>();
            params.put("timestamp", String.valueOf(getServerTime()));
            params.put("recvWindow", "5000");

            String canonical = buildCanonical(params);
            String signature = hmacSha256Hex(canonical, secret);
            params.put("signature", signature);

            String finalQuery = buildCanonical(params);
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-MEXC-APIKEY", apiKey);
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Void> entity = new HttpEntity<>(headers);

            HttpMethod httpMethod = switch (method.toUpperCase()) {
                case "GET" -> HttpMethod.GET;
                case "DELETE" -> HttpMethod.DELETE;
                default -> HttpMethod.POST;
            };

            log.info("{} https://api.mexc.com{}?{}", httpMethod.name(), path, finalQuery);
            ResponseEntity<String> resp = restTemplate.exchange(API_BASE + path + "?" + finalQuery, httpMethod, entity, String.class);
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

    public BigDecimal getTokenBalanceAccountB(String symbol, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");
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

    // ======= ORDERS =======

// Рынок BUY A с FULL-ответом (+ожидание); если не FILLED — фолбэк лимиткой НАД спредом
    public OrderInfo marketBuyAccountAFull(String symbol, BigDecimal usdtAmount, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "MARKET");
        params.put("quoteOrderQty", usdtAmount.stripTrailingZeros().toPlainString());
        params.put("newOrderRespType", "FULL");

        long t0 = System.currentTimeMillis();
        JsonNode resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        long t1 = System.currentTimeMillis();

        String orderId = resp.path("orderId").asText(null);
        String status  = resp.path("status").asText(null);

        BigDecimal executed = bd(resp.path("executedQty").asText("0"));
        BigDecimal cummQ    = bd(resp.path("cummulativeQuoteQty").asText("0"));
        BigDecimal avg      = safeAvg(cummQ, executed);

        log.info("✔️ Market BUY A {}#{}: status={}, executedQty={}, cummQuoteQty={}, avg={}, latency={}ms",
                symbol, orderId, status, executed.toPlainString(), cummQ.toPlainString(), avg.toPlainString(), (t1 - t0));

        // Если не FILLED — подождём чуть-чуть (есть уже реализованная логика)
        OrderInfo waited = ("FILLED".equals(status))
                ? new OrderInfo(orderId, status, executed, cummQ, avg)
                : waitUntilFilled(symbol, orderId, creds.getApiKey(), creds.getSecret(), 5000);

        // Успех — выходим
        if ("FILLED".equals(waited.status())) return waited;

        // Если ничего не исполнилось — аккуратно отменим и включим фолбэк
        if (waited.executedQty().compareTo(BigDecimal.ZERO) == 0) {
            tryCancelOrder(symbol, orderId, creds.getApiKey(), creds.getSecret());
            log.warn("⚠️ Market BUY A {} не дал FILLED (status={}). Делаю фолбэк: LIMIT над спредом.", symbol, waited.status());
            return limitBuyAboveSpreadAccountA(symbol, usdtAmount, chatId);
        }

        // Частичное исполнение — возвращаем как есть (не добираем остаток, чтобы не «перекупить»)
        return waited;
    }

    public String placeLimitSellAccountA(String symbol, BigDecimal price, BigDecimal qty, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal effMinNotional = resolveMinNotional(symbol, f.minNotional);

        BigDecimal normPrice = normalizePrice(price, f);
        BigDecimal normQty   = normalizeQty(qty,   f);
        BigDecimal notional  = normPrice.multiply(normQty);

        BigDecimal minQtyNeed = minQtyForNotional(normPrice, f.stepSize, effMinNotional);

        log.info("SELL {} precheck: price={} qty={} notional={} | minNotional(eff)={} minQtyForNotional={} tickSize={} stepSize={} minQty={}",
                symbol,
                normPrice.toPlainString(), normQty.toPlainString(), notional.stripTrailingZeros().toPlainString(),
                effMinNotional.stripTrailingZeros().toPlainString(), minQtyNeed.toPlainString(),
                f.tickSize.stripTrailingZeros().toPlainString(), f.stepSize.stripTrailingZeros().toPlainString(),
                f.minQty.stripTrailingZeros().toPlainString());

        if (normPrice.signum() <= 0 || normQty.signum() <= 0) {
            log.error("SELL {}: невалидные параметры: price={} qty={} (rawPrice={}, rawQty={})",
                    symbol,
                    normPrice.toPlainString(), normQty.toPlainString(),
                    price == null ? "null" : price.toPlainString(),
                    qty == null ? "null"  : qty.toPlainString());
            return null;
        }
        if (normQty.compareTo(f.minQty) < 0 || normQty.compareTo(minQtyNeed) < 0) {
            log.warn("SELL {}: qty={} < требуемого {} (или < minQty={}), ордер НЕ отправлен",
                    symbol, normQty.toPlainString(), minQtyNeed.toPlainString(), f.minQty.stripTrailingZeros().toPlainString());
            return null;
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", normQty.toPlainString());
        params.put("price",    normPrice.toPlainString());
        params.put("newOrderRespType", "ACK");

        JsonNode resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        String orderId = (resp != null && resp.has("orderId")) ? resp.get("orderId").asText() : null;

        log.info("📤 SELL {} размещён: orderId={}, price={}, qty={}, notional~{}",
                symbol, orderId, normPrice.toPlainString(), normQty.toPlainString(), notional.stripTrailingZeros().toPlainString());

        if (orderId == null) log.warn("placeLimitSellAccountA unexpected response: {}", resp);
        return orderId;
    }

    /**
     * Агрессивная LIMIT-продажа «ПОД спредом» (эмулирует MARKET SELL).
     * Ставит цену bid - N*tickSize (по умолчанию N=3) и timeInForce=IOC.
     * Возвращает итоговый статус, по возможности дожидаясь финала коротким ожиданием.
     */
    public OrderInfo limitSellBelowSpreadAccountB(String symbol, BigDecimal requestedQty, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal effMinNotional = resolveMinNotional(symbol, f.minNotional);

        // 1) Проверяем доступное количество и нормализуем под шаг
        String asset = symbol.endsWith("USDT") ? symbol.substring(0, symbol.length() - 4) : symbol;
        BigDecimal available = getAssetBalance(creds.getApiKey(), creds.getSecret(), asset);
        if (available.signum() <= 0) {
            log.warn("LIMIT SELL[AGGR] {}: у B нет доступных токенов ({})", symbol, asset);
            return new OrderInfo(null, "REJECTED", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
        }
        BigDecimal req = (requestedQty == null) ? BigDecimal.ZERO : requestedQty;
        BigDecimal capped = req.compareTo(available) <= 0 ? req : available;
        BigDecimal qty = normalizeQty(capped, f);
        if (qty.signum() <= 0) {
            log.warn("LIMIT SELL[AGGR] {}: qty<=0 после нормализации (requested={}, available={}, stepSize={})",
                    symbol, req.stripTrailingZeros(), available.stripTrailingZeros(), f.stepSize.stripTrailingZeros());
            return new OrderInfo(null, "REJECTED", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
        }

        // 2) Цена ПОД спредом — несколько тиков ниже bid
        final int ticksBelow = 3;
        BigDecimal price = priceBelowBid(symbol, ticksBelow);

        // 3) Проверяем minNotional: если не дотягиваем — увеличивать qty нельзя (продаём только то, что есть)
        if (effMinNotional.signum() > 0) {
            BigDecimal estNotional = price.multiply(qty);
            if (estNotional.compareTo(effMinNotional) < 0) {
                log.warn("LIMIT SELL[AGGR] {}: estNotional={} < minNotional(eff)={}, ордер НЕ отправлен.",
                        symbol,
                        estNotional.stripTrailingZeros().toPlainString(),
                        effMinNotional.stripTrailingZeros().toPlainString());
                return new OrderInfo(null, "REJECTED", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
            }
        }

        BigDecimal notional = price.multiply(qty);
        log.info("🔻 LIMIT SELL[AGGR] {}: placing IOC | price={} (под спредом, -{} тика) | qty={} | notional~{}",
                symbol,
                price.stripTrailingZeros().toPlainString(),
                ticksBelow,
                qty.stripTrailingZeros().toPlainString(),
                notional.stripTrailingZeros().toPlainString());

        // 4) Отправляем LIMIT IOC; если биржа не примет IOC — ретрай GTC
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "LIMIT");
        params.put("timeInForce", "IOC");
        params.put("quantity", qty.toPlainString());
        params.put("price",    price.toPlainString());
        params.put("newOrderRespType", "FULL");

        JsonNode resp;
        try {
            resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        } catch (RuntimeException ex) {
            String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";
            if (msg.contains("timeinforce")) {
                log.warn("LIMIT SELL[AGGR] {}: биржа не приняла IOC, пробую GTC", symbol);
                params.put("timeInForce", "GTC");
                resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
            } else {
                throw ex;
            }
        }

        String orderId = resp.path("orderId").asText(null);
        String status  = resp.path("status").asText("UNKNOWN");
        BigDecimal executed = bd(resp.path("executedQty").asText("0"));
        BigDecimal cummQ    = bd(resp.path("cummulativeQuoteQty").asText("0"));
        BigDecimal avg      = safeAvg(cummQ, executed);

        log.info("📤 LIMIT SELL[AGGR] {}#{} result: status={}, executedQty={}, cummQuoteQty={}, avg={}",
                symbol, orderId, status, executed.toPlainString(), cummQ.toPlainString(), avg.toPlainString());

        // 5) Если не финально — коротко подождём
        if (!"FILLED".equals(status) && !"CANCELED".equals(status) && !"REJECTED".equals(status)) {
            return waitUntilFilled(symbol, orderId, creds.getApiKey(), creds.getSecret(), 3000);
        }
        return new OrderInfo(orderId, status, executed, cummQ, avg);
    }


    // --- BUY A: перегрузка с ограничением максимального количества (под B SELL)
    public String placeLimitBuyAccountA(String symbol, BigDecimal price, BigDecimal usdtAmount, BigDecimal maxQty, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal effMinNotional = resolveMinNotional(symbol, f.minNotional);

        BigDecimal normPrice = normalizePrice(price, f);

        // сырой qty из бюджета (floor)
        BigDecimal rawQty = BigDecimal.ZERO;
        try { rawQty = usdtAmount.divide(normPrice, 18, RoundingMode.DOWN); } catch (Exception ignore) {}
        BigDecimal qty = normalizeQty(rawQty, f);

        // ограничим сверху maxQty (если задан)
        if (maxQty != null && maxQty.signum() > 0) {
            BigDecimal maxNorm = normalizeQty(maxQty, f);
            if (qty.compareTo(maxNorm) > 0) qty = maxNorm;
        }

        BigDecimal cost = qty.multiply(normPrice);
        BigDecimal minQtyNeed = minQtyForNotional(normPrice, f.stepSize, effMinNotional);

        if (qty.compareTo(minQtyNeed) < 0) {
            BigDecimal needCost = minQtyNeed.multiply(normPrice);
            if (needCost.compareTo(usdtAmount) <= 0 && (maxQty == null || minQtyNeed.compareTo(maxQty) <= 0)) {
                qty = minQtyNeed;
                cost = qty.multiply(normPrice);
            } else {
                log.warn("BUY {}: бюджет {} USDT < требуемого на minNotional {} (нужно {} USDT). Ордер НЕ отправлен.",
                        symbol, usdtAmount.stripTrailingZeros(), effMinNotional.stripTrailingZeros(), needCost.stripTrailingZeros());
                return null;
            }
        }

        // безопасность: не выходим за бюджет (после коррекций)
        if (cost.compareTo(usdtAmount) > 0) {
            qty = normalizeQty(usdtAmount.divide(normPrice, 18, RoundingMode.DOWN), f);
            // снова учесть ограничение maxQty
            if (maxQty != null && maxQty.signum() > 0) {
                BigDecimal maxNorm = normalizeQty(maxQty, f);
                if (qty.compareTo(maxNorm) > 0) qty = maxNorm;
            }
            cost = qty.multiply(normPrice);
        }

        if (qty.signum() <= 0) {
            log.warn("placeLimitBuyAccountA: qty<=0 после расчётов (budget={}, price={}, stepSize={})",
                    usdtAmount, normPrice, f.stepSize);
            return null;
        }

        log.info("BUY {} финал: price={} qty={} cost={} | rawQty={} budget={} | minNotional(eff)={} minQtyForNotional={} tickSize={} stepSize={}",
                symbol,
                normPrice.toPlainString(), qty.toPlainString(), cost.stripTrailingZeros().toPlainString(),
                rawQty.stripTrailingZeros().toPlainString(), usdtAmount.stripTrailingZeros().toPlainString(),
                effMinNotional.stripTrailingZeros().toPlainString(), minQtyNeed.toPlainString(),
                f.tickSize.stripTrailingZeros().toPlainString(), f.stepSize.stripTrailingZeros().toPlainString());

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", qty.toPlainString());
        params.put("price",    normPrice.toPlainString());
        params.put("newOrderRespType", "ACK");

        JsonNode resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        String orderId = (resp != null && resp.has("orderId")) ? resp.get("orderId").asText() : null;

        log.info("📤 BUY {} размещён: orderId={}, price={}, qty={}, cost~{}",
                symbol, orderId, normPrice.toPlainString(), qty.toPlainString(), cost.stripTrailingZeros().toPlainString());

        if (orderId == null) log.warn("placeLimitBuyAccountA unexpected response: {}", resp);
        return orderId;
    }

    // Старая сигнатура — оставлена как обёртка
    public String placeLimitBuyAccountA(String symbol, BigDecimal price, BigDecimal usdtAmount, Long chatId) {
        return placeLimitBuyAccountA(symbol, price, usdtAmount, null, chatId);
    }

    // === Планирование MARKET SELL B без отправки — чтобы согласовать с BUY A
    public BigDecimal planMarketSellQtyAccountB(String symbol, BigDecimal price, BigDecimal requestedQty, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);
        String asset = symbol.endsWith("USDT") ? symbol.substring(0, symbol.length() - 4) : symbol;
        BigDecimal available = getAssetBalance(creds.getApiKey(), creds.getSecret(), asset);

        if (available.signum() <= 0) {
            log.warn("PLAN SELL {}: у B нет доступных токенов ({})", symbol, asset);
            return BigDecimal.ZERO;
        }

        BigDecimal requested = (requestedQty == null) ? BigDecimal.ZERO : requestedQty;
        BigDecimal capped = requested.compareTo(available) <= 0 ? requested : available;
        BigDecimal normQty = normalizeQty(capped, f);

        // Проверка minNotional (если биржа требует)
        BigDecimal effMinNotional = resolveMinNotional(symbol, f.minNotional);
        if (effMinNotional.signum() > 0 && price != null && price.signum() > 0) {
            BigDecimal estNotional = price.multiply(normQty);
            if (estNotional.compareTo(effMinNotional) < 0) {
                BigDecimal needQty = effMinNotional
                        .divide(price, 0, RoundingMode.CEILING)
                        .multiply(f.stepSize);
                BigDecimal multiples = needQty.divide(f.stepSize, 0, RoundingMode.CEILING);
                needQty = multiples.multiply(f.stepSize);

                if (needQty.compareTo(available) <= 0) {
                    normQty = needQty;
                } else {
                    log.warn("PLAN SELL {}: minNotional={} не покрывается: доступно {} {}, требуется qty={} при price={}.",
                            symbol,
                            effMinNotional.stripTrailingZeros().toPlainString(),
                            available.stripTrailingZeros().toPlainString(), asset,
                            needQty.stripTrailingZeros().toPlainString(),
                            price.stripTrailingZeros().toPlainString());
                    return BigDecimal.ZERO;
                }
            }
        }

        log.info("PLAN SELL {}: qty={} | requested={} available={} | stepSize={} minNotional(eff)={}",
                symbol,
                normQty.toPlainString(),
                requested.stripTrailingZeros().toPlainString(),
                available.stripTrailingZeros().toPlainString(),
                f.stepSize.stripTrailingZeros().toPlainString(),
                resolveMinNotional(symbol, f.minNotional).stripTrailingZeros().toPlainString());

        return normQty;
    }

    // Рынок BUY B (учитываем комиссию в требуемой сумме, плюс запас)
    public void marketBuyFromAccountB(String symbol, BigDecimal price, BigDecimal qty, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);
        int quoteScale = resolveQuoteScale(symbol, f);

        // 1) Оценка требуемой суммы (с учётом комиссии taker + safety)
        BigDecimal requiredUsdt = BigDecimal.ZERO;
        try {
            BigDecimal base = price.multiply(qty);
            requiredUsdt = addFeeUp(base, TAKER_FEE, FEE_SAFETY);
        } catch (Exception ex) {
            log.warn("Ошибка расчёта requiredUsdt: {}", ex.getMessage(), ex);
        }

        // 2) Бюджет на B
        BigDecimal availableUsdt = getAssetBalance(creds.getApiKey(), creds.getSecret(), "USDT");
        if (availableUsdt.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("B не имеет USDT для покупки (available=0)");
            return;
        }

        // Если бюджета меньше — уменьшаем qty пропорционально бюджету
        if (availableUsdt.compareTo(requiredUsdt) < 0) {
            BigDecimal adjustedQty = BigDecimal.ZERO;
            try {
                // обратный расчёт по цене и (1+fee)
                BigDecimal denom = addFeeUp(price, TAKER_FEE, FEE_SAFETY); // цена с накидкой
                adjustedQty = availableUsdt.divide(denom, 18, RoundingMode.DOWN);
            } catch (Exception ignore) {}
            if (adjustedQty.compareTo(BigDecimal.ZERO) <= 0) {
                log.warn("B недостаточно USDT ({}) чтобы купить хоть немного токенов по цене {}", availableUsdt, price);
                return;
            }
            log.info("B имеет меньше USDT ({}) чем нужно ({}). Уменьшаем qty -> {}",
                    availableUsdt.stripTrailingZeros().toPlainString(),
                    requiredUsdt.stripTrailingZeros().toPlainString(),
                    adjustedQty.stripTrailingZeros().toPlainString());
            qty = adjustedQty;
            requiredUsdt = availableUsdt;
        }

        // 3) Нормализуем quoteOrderQty по точности котируемой валюты
        BigDecimal quote = normalizeQuoteAmount(requiredUsdt, quoteScale);

        log.info("MARKET BUY[B] {} precheck: price={} qty={} requiredUsdt={} -> quote(norm,scale={})={}",
                symbol,
                price.stripTrailingZeros().toPlainString(),
                qty.stripTrailingZeros().toPlainString(),
                requiredUsdt.stripTrailingZeros().toPlainString(),
                quoteScale, quote.toPlainString());

        // 4) Порог 1 USDT (эффективный)
        BigDecimal effMinNotional = resolveMinNotional(symbol, f.minNotional);
        if (symbol.endsWith("USDT") && quote.compareTo(effMinNotional) < 0) {
            log.warn("MARKET BUY[B] {}: quote={} < minNotional={} USDT — ордер НЕ отправлен.",
                    symbol, quote.toPlainString(), effMinNotional.toPlainString());
            return;
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "MARKET");
        params.put("quoteOrderQty", quote.toPlainString());

        // 5) Отправка + авто-ретрай при "amount scale is invalid"
        try {
            signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        } catch (RuntimeException ex) {
            String msg = ex.getMessage() != null ? ex.getMessage() : "";
            if (msg.contains("amount scale is invalid") || msg.contains("scale is invalid")) {
                int[] fallbacks = {Math.min(quoteScale, 8), 6, 4, 2, 0};
                for (int s : fallbacks) {
                    if (s == quoteScale) continue;
                    BigDecimal q2 = normalizeQuoteAmount(requiredUsdt.min(availableUsdt), s);
                    if (symbol.endsWith("USDT") && q2.compareTo(effMinNotional) < 0) {
                        continue;
                    }
                    log.warn("MARKET BUY[B] {}: ретрай с более грубым scale={}, quote={}", symbol, s, q2.toPlainString());
                    params.put("quoteOrderQty", q2.toPlainString());
                    try {
                        signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
                        return;
                    } catch (RuntimeException ex2) {
                        String m2 = ex2.getMessage() != null ? ex2.getMessage() : "";
                        if (!(m2.contains("amount scale is invalid") || m2.contains("scale is invalid"))) {
                            throw ex2;
                        }
                    }
                }
            }
            throw ex;
        }
    }

    // MARKET SELL B — теперь используем ровно планируемое количество
    // MARKET SELL B — теперь с FULL-ответом, ожиданием и фолбэком лимиткой ПОД спредом (IOC)
    public void marketSellFromAccountB(String symbol, BigDecimal price, BigDecimal qty, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);

        // 1) Доступный баланс и нормализация количества
        String asset = symbol.endsWith("USDT") ? symbol.substring(0, symbol.length() - 4) : symbol;
        BigDecimal available = getAssetBalance(creds.getApiKey(), creds.getSecret(), asset);
        if (available.signum() <= 0) {
            log.warn("MARKET SELL {}: у B нет доступных токенов ({})", symbol, asset);
            return;
        }
        BigDecimal requested = (qty == null) ? BigDecimal.ZERO : qty;
        BigDecimal capped = requested.compareTo(available) <= 0 ? requested : available;
        BigDecimal normQty = normalizeQty(capped, f);
        if (normQty.signum() <= 0) {
            log.warn("MARKET SELL {}: рассчитанное qty <= 0 (requested={}, available={}, stepSize={})",
                    symbol,
                    requested.stripTrailingZeros().toPlainString(),
                    available.stripTrailingZeros().toPlainString(),
                    f.stepSize.stripTrailingZeros().toPlainString());
            return;
        }

        // 2) Проверка minNotional при известной «подсказочной» цене
        BigDecimal effMinNotional = resolveMinNotional(symbol, f.minNotional);
        if (effMinNotional.signum() > 0 && price != null && price.signum() > 0) {
            BigDecimal estNotional = price.multiply(normQty);
            if (estNotional.compareTo(effMinNotional) < 0) {
                log.warn("MARKET SELL {}: estNotional={} < minNotional(eff)={}, ордер не отправлен.",
                        symbol, estNotional.stripTrailingZeros().toPlainString(), effMinNotional.toPlainString());
                return;
            }
        }

        log.info("MARKET SELL {}: финальные параметры -> qty={} | requested={} available={} | stepSize={} minQty={} minNotional={}",
                symbol,
                normQty.toPlainString(),
                requested.stripTrailingZeros().toPlainString(),
                available.stripTrailingZeros().toPlainString(),
                f.stepSize.stripTrailingZeros().toPlainString(),
                f.minQty.stripTrailingZeros().toPlainString(),
                f.minNotional.stripTrailingZeros().toPlainString()
        );

        // 3) Пробуем MARKET с FULL-ответом
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "MARKET");
        params.put("quantity", normQty.toPlainString());
        params.put("newOrderRespType", "FULL");

        JsonNode resp;
        try {
            resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        } catch (RuntimeException ex) {
            log.warn("⚠️ MARKET SELL {}: ошибка при отправке MARKET ({}). Делаю фолбэк: LIMIT под спредом.", symbol, ex.getMessage());
            limitSellBelowSpreadAccountB(symbol, normQty, chatId);
            return;
        }

        String orderId = resp.path("orderId").asText(null);
        String status  = resp.path("status").asText("UNKNOWN");
        BigDecimal executed = bd(resp.path("executedQty").asText("0"));
        BigDecimal cummQ    = bd(resp.path("cummulativeQuoteQty").asText("0"));
        BigDecimal avg      = safeAvg(cummQ, executed);

        log.info("✔️ MARKET SELL {}#{}: status={}, executedQty={}, cummQuoteQty={}, avg={}",
                symbol, orderId, status, executed.toPlainString(), cummQ.toPlainString(), avg.toPlainString());

        // 4) Если не FILLED — подождём чуть-чуть
        OrderInfo waited = ("FILLED".equals(status))
                ? new OrderInfo(orderId, status, executed, cummQ, avg)
                : waitUntilFilled(symbol, orderId, creds.getApiKey(), creds.getSecret(), 5000);

        // Успех — выходим
        if ("FILLED".equals(waited.status())) return;

        // Если совсем не исполнилось — отменяем и делаем фолбэк лимиткой ПОД спредом (IOC)
        if (waited.executedQty().compareTo(BigDecimal.ZERO) == 0) {
            tryCancelOrder(symbol, orderId, creds.getApiKey(), creds.getSecret());
            log.warn("⚠️ MARKET SELL {} не дал FILLED (status={}). Делаю фолбэк: LIMIT под спредом.", symbol, waited.status());
            limitSellBelowSpreadAccountB(symbol, normQty, chatId);
            return;
        }

        // Частичный FILLED — оставляем как есть (не дожимаем остаток, чтобы не переселлить)
    }


    public void forceMarketSellAccountA(String symbol, BigDecimal qty, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "MARKET");
        params.put("quantity", qty.setScale(5, RoundingMode.DOWN).toPlainString());

        signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
    }

    // ======= SPREAD helpers =======

    public BigDecimal getNearLowerSpreadPrice(String symbol) {
        try {
            String url = API_BASE + TICKER_BOOK + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode resp = objectMapper.readTree(body);

            BigDecimal bid = new BigDecimal(resp.path("bidPrice").asText("0"));
            BigDecimal ask = new BigDecimal(resp.path("askPrice").asText("0"));

            if (bid.signum() <= 0 && ask.signum() > 0) bid = ask;
            else if (ask.signum() <= 0 && bid.signum() > 0) ask = bid;
            else if (bid.signum() <= 0 && ask.signum() <= 0) {
                SymbolFilters f = getSymbolFilters(symbol);
                BigDecimal p = f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
                log.warn("Пустой стакан {} — возвращаю 1 тик: {}", symbol, p);
                return p;
            }

            BigDecimal spread = ask.subtract(bid);
            if (spread.signum() < 0) spread = BigDecimal.ZERO;

            BigDecimal raw = bid.add(spread.multiply(new BigDecimal("0.10")));
            SymbolFilters f = getSymbolFilters(symbol);
            BigDecimal price = normalizePrice(raw, f);

            log.info("getNearLowerSpreadPrice[{}]: bid={} ask={} spread={} raw={} -> normalized={} (tickSize={})",
                    symbol, bid.stripTrailingZeros().toPlainString(),
                    ask.stripTrailingZeros().toPlainString(),
                    spread.stripTrailingZeros().toPlainString(),
                    raw.stripTrailingZeros().toPlainString(),
                    price.toPlainString(),
                    f.tickSize.stripTrailingZeros().toPlainString());

            return price;
        } catch (Exception e) {
            log.error("Ошибка получения стакана для {}: {}", symbol, e.getMessage(), e);
            SymbolFilters f = getSymbolFilters(symbol);
            return f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
        }
    }

    public BigDecimal getNearUpperSpreadPrice(String symbol) {
        try {
            String url = API_BASE + TICKER_BOOK + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode resp = objectMapper.readTree(body);

            BigDecimal bid = new BigDecimal(resp.path("bidPrice").asText("0"));
            BigDecimal ask = new BigDecimal(resp.path("askPrice").asText("0"));

            if (bid.signum() <= 0 && ask.signum() > 0) bid = ask;
            else if (ask.signum() <= 0 && bid.signum() > 0) ask = bid;
            else if (bid.signum() <= 0 && ask.signum() <= 0) {
                SymbolFilters f = getSymbolFilters(symbol);
                BigDecimal p = f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
                log.warn("Пустой стакан {} — возвращаю 1 тик (upper): {}", symbol, p);
                return p;
            }

            BigDecimal spread = ask.subtract(bid);
            if (spread.signum() < 0) spread = BigDecimal.ZERO;

            BigDecimal raw = ask.subtract(spread.multiply(new BigDecimal("0.10")));
            SymbolFilters f = getSymbolFilters(symbol);
            BigDecimal price = normalizePrice(raw, f);

            log.info("getNearUpperSpreadPrice[{}]: bid={} ask={} spread={} raw={} -> normalized={} (tickSize={})",
                    symbol,
                    bid.stripTrailingZeros().toPlainString(),
                    ask.stripTrailingZeros().toPlainString(),
                    spread.stripTrailingZeros().toPlainString(),
                    raw.stripTrailingZeros().toPlainString(),
                    price.toPlainString(),
                    f.tickSize.stripTrailingZeros().toPlainString());

            return price;
        } catch (Exception e) {
            log.error("Ошибка получения стакана (upper) для {}: {}", symbol, e.getMessage(), e);
            SymbolFilters f = getSymbolFilters(symbol);
            return f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
        }
    }

    // Внутри MexcTradeService.java — ДОБАВИТЬ:

    public BigDecimal getUsdtBalanceAccountB(Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");
        return getAssetBalance(creds.getApiKey(), creds.getSecret(), "USDT");
    }

    /**
     * Перегрузка: вернуть фактически выставленный quoteOrderQty (для счётчика).
     * Старая marketBuyFromAccountB(...) не трогаю.
     */
    public BigDecimal marketBuyFromAccountB(String symbol, BigDecimal price, BigDecimal qty, Long chatId, boolean returnSpent) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);
        int quoteScale = resolveQuoteScale(symbol, f);

        BigDecimal requiredUsdt;
        try {
            requiredUsdt = addFeeUp(price.multiply(qty), TAKER_FEE, FEE_SAFETY);
        } catch (Exception ex) {
            log.warn("Ошибка расчёта requiredUsdt: {}", ex.getMessage(), ex);
            requiredUsdt = BigDecimal.ZERO;
        }

        BigDecimal availableUsdt = getAssetBalance(creds.getApiKey(), creds.getSecret(), "USDT");
        if (availableUsdt.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("B не имеет USDT для покупки (available=0)");
            return BigDecimal.ZERO;
        }

        BigDecimal quote = normalizeQuoteAmount(requiredUsdt.min(availableUsdt), quoteScale);

        // проверка minNotional
        BigDecimal effMinNotional = resolveMinNotional(symbol, f.minNotional);
        if (symbol.endsWith("USDT") && quote.compareTo(effMinNotional) < 0) {
            log.warn("MARKET BUY[B] {}: quote={} < minNotional={} USDT — ордер НЕ отправлен.",
                    symbol, quote.toPlainString(), effMinNotional.toPlainString());
            return BigDecimal.ZERO;
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "MARKET");
        params.put("quoteOrderQty", quote.toPlainString());

        try {
            signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
            log.info("MARKET BUY[B] {}: отправил quoteOrderQty={}", symbol, quote.toPlainString());
            return returnSpent ? quote : BigDecimal.ZERO;
        } catch (RuntimeException ex) {
            String msg = ex.getMessage() != null ? ex.getMessage() : "";
            if (msg.contains("amount scale is invalid") || msg.contains("scale is invalid")) {
                int[] fallbacks = {Math.min(quoteScale, 8), 6, 4, 2, 0};
                for (int s : fallbacks) {
                    if (s == quoteScale) continue;
                    BigDecimal q2 = normalizeQuoteAmount(requiredUsdt.min(availableUsdt), s);
                    if (symbol.endsWith("USDT") && q2.compareTo(effMinNotional) < 0) continue;
                    log.warn("MARKET BUY[B] {}: ретрай с scale={}, quote={}", symbol, s, q2.toPlainString());
                    params.put("quoteOrderQty", q2.toPlainString());
                    try {
                        signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
                        return returnSpent ? q2 : BigDecimal.ZERO;
                    } catch (RuntimeException ex2) {
                        String m2 = ex2.getMessage() != null ? ex2.getMessage() : "";
                        if (!(m2.contains("amount scale is invalid") || m2.contains("scale is invalid"))) {
                            throw ex2;
                        }
                    }
                }
            }
            throw ex;
        }
    }

    // ======= Тех. методы =======
    // --- DTO для открытых ордеров и глубины
    public record OpenOrder(
            String orderId,
            String side,            // "BUY" | "SELL"
            BigDecimal price,
            BigDecimal origQty,
            BigDecimal executedQty
    ) {
        public BigDecimal remaining() {
            if (origQty == null || executedQty == null) return BigDecimal.ZERO;
            BigDecimal r = origQty.subtract(executedQty);
            return r.signum() > 0 ? r.stripTrailingZeros() : BigDecimal.ZERO;
        }
    }

    public record DepthSnapshot(
            long lastUpdateId,
            List<Level> bids,       // убывание цены
            List<Level> asks        // возрастание цены
    ) {
        public record Level(BigDecimal price, BigDecimal qty) {}
    }

    public record TopOfBook(BigDecimal bid, BigDecimal ask) {}

    // --- Открытые ордера аккаунта A по символу
    public List<OpenOrder> getOpenOrdersAccountA(String symbol, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        Map<String, String> p = new LinkedHashMap<>();
        p.put("symbol", symbol);

        JsonNode resp = signedRequest("GET", API_PREFIX + "/openOrders", p, creds.getApiKey(), creds.getSecret());
        List<OpenOrder> out = new ArrayList<>();
        if (resp != null && resp.isArray()) {
            for (JsonNode n : resp) {
                String orderId = n.path("orderId").asText(null);
                String side    = n.path("side").asText(null);
                BigDecimal price = bd(n.path("price").asText("0"));
                BigDecimal orig  = bd(n.path("origQty").asText("0"));
                BigDecimal exec  = bd(n.path("executedQty").asText("0"));
                out.add(new OpenOrder(orderId, side, price, orig, exec));
            }
        }
        return out;
    }

    // --- Снимок глубины (REST)
    public DepthSnapshot getDepth(String symbol, int limit) {
        try {
            String url = API_BASE + "/api/v3/depth?symbol=" + symbol + "&limit=" + Math.min(Math.max(limit, 5), 100);
            String body = restTemplate.getForObject(url, String.class);
            JsonNode j = objectMapper.readTree(body);

            long u = j.path("lastUpdateId").asLong(0);

            List<DepthSnapshot.Level> bids = new ArrayList<>();
            JsonNode jb = j.path("bids");
            if (jb != null && jb.isArray()) {
                for (JsonNode row : jb) {
                    BigDecimal price = bd(row.get(0).asText("0"));
                    BigDecimal qty   = bd(row.get(1).asText("0"));
                    if (price.signum() > 0 && qty.signum() > 0)
                        bids.add(new DepthSnapshot.Level(price, qty));
                }
            }

            List<DepthSnapshot.Level> asks = new ArrayList<>();
            JsonNode ja = j.path("asks");
            if (ja != null && ja.isArray()) {
                for (JsonNode row : ja) {
                    BigDecimal price = bd(row.get(0).asText("0"));
                    BigDecimal qty   = bd(row.get(1).asText("0"));
                    if (price.signum() > 0 && qty.signum() > 0)
                        asks.add(new DepthSnapshot.Level(price, qty));
                }
            }
            return new DepthSnapshot(u, bids, asks);
        } catch (Exception e) {
            log.warn("getDepth[{}] error: {}", symbol, e.getMessage());
            return new DepthSnapshot(0L, List.of(), List.of());
        }
    }

    /**
     * Лучший bid/ask БЕЗ учёта собственных лимиток аккаунта A.
     * Алгоритм: идём по bid (сверху вниз) / ask (снизу вверх), для уровня
     * вычитаем суммарный остаток собственных ордеров на этой цене; если остатка
     * у «чужих» > 0 — это наш «ex-self» топ. Иначе берём следующий уровень.
     */
    public TopOfBook topExcludingSelf(String symbol, Long chatId, int depthLimit) {
        DepthSnapshot d = getDepth(symbol, depthLimit);
        if (d.bids().isEmpty() || d.asks().isEmpty())
            return new TopOfBook(BigDecimal.ZERO, BigDecimal.ZERO);

        // Собираем остатки собственных BUY/SELL по ценам
        Map<BigDecimal, BigDecimal> selfBidRest = new HashMap<>();
        Map<BigDecimal, BigDecimal> selfAskRest = new HashMap<>();
        for (OpenOrder o : getOpenOrdersAccountA(symbol, chatId)) {
            BigDecimal r = o.remaining();
            if (r.signum() <= 0) continue;
            if ("BUY".equalsIgnoreCase(o.side())) {
                selfBidRest.merge(o.price(), r, BigDecimal::add);
            } else if ("SELL".equalsIgnoreCase(o.side())) {
                selfAskRest.merge(o.price(), r, BigDecimal::add);
            }
        }

        BigDecimal bestBid = BigDecimal.ZERO;
        for (DepthSnapshot.Level lvl : d.bids()) {
            BigDecimal net = lvl.qty().subtract(selfBidRest.getOrDefault(lvl.price(), BigDecimal.ZERO));
            if (net.signum() > 0) { bestBid = lvl.price(); break; }
        }

        BigDecimal bestAsk = BigDecimal.ZERO;
        for (DepthSnapshot.Level lvl : d.asks()) {
            BigDecimal net = lvl.qty().subtract(selfAskRest.getOrDefault(lvl.price(), BigDecimal.ZERO));
            if (net.signum() > 0) { bestAsk = lvl.price(); break; }
        }

        // Fallback: если весь верх — это только ты
        if (bestBid.signum() == 0 && !d.bids().isEmpty()) bestBid = d.bids().get(0).price();
        if (bestAsk.signum() == 0 && !d.asks().isEmpty()) bestAsk = d.asks().get(0).price();

        return new TopOfBook(bestBid, bestAsk);
    }

    /** Привести цену к сетке тика (floor). Безопасно для BUY/SELL, когда нужно не превышать raw. */
    public BigDecimal alignPriceFloor(String symbol, BigDecimal rawPrice) {
        return normalizePrice(rawPrice, getSymbolFilters(symbol));
    }

    /** Привести цену к «ceil» сетки тика: ближайший допустимый тик НЕ НИЖЕ raw. Удобно для SELL у нижней кромки. */
    public BigDecimal alignPriceCeil(String symbol, BigDecimal rawPrice) {
        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal p = normalizePrice(rawPrice, f); // floor
        if (rawPrice != null && p.compareTo(rawPrice) < 0) {
            p = p.add(f.tickSize);
            p = normalizePrice(p, f); // перестраховка
        }
        return p.stripTrailingZeros();
    }

    /** Привести количество к сетке шага лота (floor). */
    public BigDecimal alignQtyFloor(String symbol, BigDecimal rawQty) {
        return normalizeQty(rawQty, getSymbolFilters(symbol));
    }

    /** Округляет value ВНИЗ до ближайшего кратного step (floor к сетке). */
    private static BigDecimal floorToStep(BigDecimal value, BigDecimal step) {
        if (value == null || step == null || step.signum() <= 0) return value;
        if (value.signum() <= 0) return BigDecimal.ZERO;
        BigDecimal multiples = value.divide(step, 0, RoundingMode.DOWN);
        return multiples.multiply(step);
    }

    /** Корректирует и валидирует цену (не даём уйти в 0). */
    private static BigDecimal normalizePrice(BigDecimal rawPrice, SymbolFilters f) {
        BigDecimal p = floorToStep(rawPrice, f.tickSize);
        if (p == null || p.signum() <= 0) {
            p = f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
        }
        return p.stripTrailingZeros();
    }

    /** Корректирует и валидирует количество (до кратности stepSize). */
    private static BigDecimal normalizeQty(BigDecimal rawQty, SymbolFilters f) {
        BigDecimal q = floorToStep(rawQty, f.stepSize);
        if (q == null) q = BigDecimal.ZERO;

        if (f.minQty.signum() > 0 && q.signum() > 0 && q.compareTo(f.minQty) < 0) {
            BigDecimal neededMultiples = f.minQty.divide(f.stepSize, 0, RoundingMode.CEILING);
            q = neededMultiples.multiply(f.stepSize);
            if (q.compareTo(rawQty) > 0) {
                q = floorToStep(rawQty, f.stepSize);
            }
        }
        return q.stripTrailingZeros();
    }

    /** Грубо проверяем notional, если фильтр присутствует (может отсутствовать на MEXC). */
    @SuppressWarnings("unused")
    private static boolean satisfiesNotional(BigDecimal price, BigDecimal qty, SymbolFilters f) {
        if (f.minNotional.signum() <= 0) return true;
        if (price == null || qty == null) return false;
        BigDecimal notional = price.multiply(qty);
        return notional.compareTo(f.minNotional) >= 0;
    }

    /** Получить фильтры символа (с кэшем) */
    private SymbolFilters getSymbolFilters(String symbol) {
        long now = System.currentTimeMillis();

        CachedSymbolInfo cached = exchangeInfoCache.get(symbol);
        if (cached != null && (now - cached.loadedAt) < EXCHANGE_INFO_TTL_MS) {
            return cached.filters;
        }

        try {
            String url = API_BASE + EXCHANGE_INFO_ENDPOINT + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode json = objectMapper.readTree(body);

            JsonNode symbols = json.get("symbols");
            if (symbols == null || !symbols.isArray() || symbols.isEmpty()) {
                log.warn("exchangeInfo: пустой symbols для {}", symbol);
                SymbolFilters def = new SymbolFilters(
                        new BigDecimal("0.00000001"),
                        BigDecimal.ONE,
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        symbol.endsWith("USDT") ? 6 : 8
                );
                exchangeInfoCache.put(symbol, new CachedSymbolInfo(def, now));
                return def;
            }

            JsonNode s0 = symbols.get(0);

            Integer quotePrecision = null;
            if (s0.has("quotePrecision")) {
                quotePrecision = s0.get("quotePrecision").asInt();
            } else if (s0.has("quoteAssetPrecision")) {
                quotePrecision = s0.get("quoteAssetPrecision").asInt();
            }

            BigDecimal tickSize = null;
            BigDecimal stepSize = null;
            BigDecimal minQty   = null;
            BigDecimal minNotional = null;

            JsonNode filters = s0.get("filters");
            if (filters != null && filters.isArray()) {
                for (JsonNode f : filters) {
                    String type = f.path("filterType").asText("");
                    switch (type) {
                        case "PRICE_FILTER" -> tickSize = new BigDecimal(f.path("tickSize").asText("0.00000001"));
                        case "LOT_SIZE" -> {
                            stepSize = new BigDecimal(f.path("stepSize").asText("1"));
                            minQty   = new BigDecimal(f.path("minQty").asText("0"));
                        }
                        case "MIN_NOTIONAL", "NOTIONAL" -> {
                            String v = f.has("minNotional") ? f.path("minNotional").asText("0")
                                    : f.has("minNotionalValue") ? f.path("minNotionalValue").asText("0")
                                    : f.path("minNotional").asText("0");
                            minNotional = new BigDecimal(v);
                        }
                        default -> { /* ignore */ }
                    }
                }
            }

            if (tickSize == null) tickSize = new BigDecimal("0.00000001");
            if (stepSize == null) stepSize = BigDecimal.ONE;
            if (minQty == null)   minQty   = BigDecimal.ZERO;
            if (minNotional == null) minNotional = BigDecimal.ZERO;
            if (quotePrecision == null) {
                quotePrecision = symbol.endsWith("USDT") ? 6 : 8;
            }

            SymbolFilters parsed = new SymbolFilters(tickSize, stepSize, minQty, minNotional, quotePrecision);
            exchangeInfoCache.put(symbol, new CachedSymbolInfo(parsed, now));

            log.info("exchangeInfo[{}]: tickSize={}, stepSize={}, minQty={}, minNotional={}, quotePrecision={}",
                    symbol,
                    parsed.tickSize.stripTrailingZeros().toPlainString(),
                    parsed.stepSize.stripTrailingZeros().toPlainString(),
                    parsed.minQty.stripTrailingZeros().toPlainString(),
                    parsed.minNotional.stripTrailingZeros().toPlainString(),
                    parsed.quotePrecision);

            return parsed;

        } catch (Exception e) {
            log.error("Ошибка exchangeInfo для {}: {}", symbol, e.getMessage(), e);
            SymbolFilters def = new SymbolFilters(
                    new BigDecimal("0.00000001"),
                    BigDecimal.ONE,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    symbol.endsWith("USDT") ? 6 : 8
            );
            exchangeInfoCache.put(symbol, new CachedSymbolInfo(def, now));
            return def;
        }
    }

    /** Сколько знаков допустимо у quote (USDT) для данного символа. */
    private static int resolveQuoteScale(String symbol, SymbolFilters f) {
        if (f != null && f.quotePrecision != null && f.quotePrecision > 0) {
            return f.quotePrecision;
        }
        return (symbol != null && symbol.endsWith("USDT")) ? 6 : 8;
    }

    /** Обрезаем сумму вниз до допустимого количества знаков у quote. */
    private static BigDecimal normalizeQuoteAmount(BigDecimal amount, int quoteScale) {
        if (amount == null) return BigDecimal.ZERO;
        if (quoteScale < 0) quoteScale = 0;
        return amount.setScale(quoteScale, RoundingMode.DOWN).stripTrailingZeros();
    }

    // -- Модель результата ордера
    public record OrderInfo(
            String orderId,
            String status,               // NEW / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED
            BigDecimal executedQty,      // сколько базовой монеты реально исполнено
            BigDecimal cummQuoteQty,     // сколько USDT списано/получено фактически
            BigDecimal avgPrice          // средняя цена (cummQuoteQty / executedQty)
    ) {}

    private static BigDecimal bd(String s) { return new BigDecimal(s).stripTrailingZeros(); }

    private static BigDecimal safeAvg(BigDecimal quote, BigDecimal base) {
        return (base == null || base.signum() == 0) ? BigDecimal.ZERO
                : quote.divide(base, 12, RoundingMode.HALF_UP).stripTrailingZeros();
    }

    // -- Ждём пока ордер станет финальным
    OrderInfo waitUntilFilled(String symbol, String orderId, String apiKey, String secret, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long[] sleeps = {150, 300, 600, 900, 1200};
        int i = 0;
        while (true) {
            Map<String, String> q = new LinkedHashMap<>();
            q.put("symbol", symbol);
            q.put("orderId", orderId);

            JsonNode r = signedRequest("GET", ORDER_ENDPOINT, q, apiKey, secret);

            String status = r.path("status").asText("UNKNOWN");
            BigDecimal executed = bd(r.path("executedQty").asText("0"));
            BigDecimal cummQ    = bd(r.path("cummulativeQuoteQty").asText("0"));
            BigDecimal avg      = safeAvg(cummQ, executed);

            log.info("⏳ Ожидание FILLED {}#{}: status={}, executedQty={}, cummQuoteQty={}, avg={}",
                    symbol, orderId, status, executed.toPlainString(), cummQ.toPlainString(), avg.toPlainString());

            if ("FILLED".equals(status) || "CANCELED".equals(status) || "REJECTED".equals(status)) {
                return new OrderInfo(orderId, status, executed, cummQ, avg);
            }
            if (System.currentTimeMillis() > deadline) {
                log.warn("⏱ Таймаут ожидания FILLED {}#{}. Последний статус={}", symbol, orderId, status);
                return new OrderInfo(orderId, status, executed, cummQ, avg);
            }
            try { Thread.sleep(sleeps[Math.min(i++, sleeps.length - 1)]); } catch (InterruptedException ignored) {}
        }
    }

    // -- Эффективный minNotional: если биржа не отдала, используем дефолт для USDT-пар
    private static BigDecimal resolveMinNotional(String symbol, BigDecimal exMinNotional) {
        if (exMinNotional != null && exMinNotional.compareTo(BigDecimal.ZERO) > 0) return exMinNotional;
        return (symbol != null && symbol.endsWith("USDT")) ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    // -- Минимально допустимое qty при заданной цене под minNotional (кратно stepSize)
    private static BigDecimal minQtyForNotional(BigDecimal price, BigDecimal stepSize, BigDecimal minNotional) {
        if (price == null || price.signum() <= 0) return BigDecimal.ZERO;
        if (minNotional == null || minNotional.signum() <= 0) return BigDecimal.ZERO;
        if (stepSize == null || stepSize.signum() <= 0) stepSize = BigDecimal.ONE;

        BigDecimal units = minNotional.divide(price, 0, RoundingMode.UP);
        BigDecimal k = units.divide(stepSize, 0, RoundingMode.UP);
        return k.multiply(stepSize).stripTrailingZeros();
    }

    // ======= Fee helpers =======

    /** Накидываем комиссию (для требуемой суммы): amount * (1 + fee + safety). */
    private static BigDecimal addFeeUp(BigDecimal amount, BigDecimal fee, BigDecimal safety) {
        if (amount == null) return BigDecimal.ZERO;
        BigDecimal k = BigDecimal.ONE.add(fee).add(safety);
        return amount.multiply(k);
    }

    /** Резерв под комиссию (для бюджета): amount * (1 - fee - safety). */
    public BigDecimal reserveForMakerFee(BigDecimal amount) {
        if (amount == null) return BigDecimal.ZERO;
        BigDecimal k = BigDecimal.ONE.subtract(MAKER_FEE).subtract(FEE_SAFETY);
        if (k.compareTo(BigDecimal.ZERO) <= 0) k = new BigDecimal("0.99");
        return amount.multiply(k);
    }
    /** Котировка стакана (best bid/ask) */
    private record BookTicker(BigDecimal bid, BigDecimal ask) {}

    private BookTicker fetchBookTicker(String symbol) {
        try {
            String url = API_BASE + TICKER_BOOK + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode resp = objectMapper.readTree(body);

            BigDecimal bid = new BigDecimal(resp.path("bidPrice").asText("0"));
            BigDecimal ask = new BigDecimal(resp.path("askPrice").asText("0"));

            // Если стакан пустой с одной стороны — зеркалим, чтобы не получить нули
            if (bid.signum() <= 0 && ask.signum() > 0) bid = ask;
            if (ask.signum() <= 0 && bid.signum() > 0) ask = bid;
            if (bid.signum() <= 0 && ask.signum() <= 0) {
                // Совсем пусто — вернём 1 тик, чтобы не упасть
                SymbolFilters f = getSymbolFilters(symbol);
                BigDecimal p = f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
                return new BookTicker(p, p);
            }
            return new BookTicker(bid, ask);
        } catch (Exception e) {
            log.error("Ошибка чтения bookTicker для {}: {}", symbol, e.getMessage());
            SymbolFilters f = getSymbolFilters(symbol);
            BigDecimal p = f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
            return new BookTicker(p, p);
        }
    }

    /**
     * Агрессивная LIMIT-покупка «НАД спредом» (эмулирует MARKET).
     * Берём ask и ставим цену ask + N*tickSize (по умолчанию N=3).
     * Отправляем LIMIT IOC (если биржа не примет IOC — пробуем GTC).
     * Возвращаем OrderInfo с финальным статусом (по возможности).
     */
    public OrderInfo limitBuyAboveSpreadAccountA(String symbol, BigDecimal usdtAmount, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal effMinNotional = resolveMinNotional(symbol, f.minNotional);

        // Цена над спредом — несколько тиков выше ask
        final int ticksAbove = 3;
        BigDecimal price = priceAboveAsk(symbol, ticksAbove);

        // Считаем количество «с запасом вниз», чтобы не выйти за бюджет
        BigDecimal rawQty = BigDecimal.ZERO;
        try { rawQty = usdtAmount.divide(price, 18, RoundingMode.DOWN); } catch (Exception ignore) {}
        BigDecimal qty = normalizeQty(rawQty, f);

        // Проверки minQty и minNotional
        BigDecimal minQtyNeed = minQtyForNotional(price, f.stepSize, effMinNotional);
        if (qty.compareTo(minQtyNeed) < 0) {
            BigDecimal needCost = minQtyNeed.multiply(price);
            if (needCost.compareTo(usdtAmount) <= 0) {
                qty = minQtyNeed;
            } else {
                log.warn("LIMIT BUY[AGGR] {}: бюджет {} USDT < требуемого на minNotional {} (нужно {} USDT). Ордер НЕ отправлен.",
                        symbol,
                        usdtAmount.stripTrailingZeros().toPlainString(),
                        effMinNotional.stripTrailingZeros().toPlainString(),
                        needCost.stripTrailingZeros().toPlainString());
                return new OrderInfo(null, "REJECTED", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
            }
        }
        if (qty.signum() <= 0) {
            log.warn("LIMIT BUY[AGGR] {}: qty<=0 после расчётов (budget={}, price={}, stepSize={})",
                    symbol, usdtAmount, price, f.stepSize);
            return new OrderInfo(null, "REJECTED", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
        }

        BigDecimal notional = price.multiply(qty);
        log.info("🟢 LIMIT BUY[AGGR] {}: placing IOC | price={} (над спредом, +{} тика) | qty={} | notional~{}",
                symbol,
                price.stripTrailingZeros().toPlainString(),
                ticksAbove,
                qty.stripTrailingZeros().toPlainString(),
                notional.stripTrailingZeros().toPlainString());

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "LIMIT");
        params.put("timeInForce", "IOC"); // хотим «немедленно или отменить»
        params.put("quantity", qty.toPlainString());
        params.put("price",    price.toPlainString());
        params.put("newOrderRespType", "FULL");

        JsonNode resp;
        try {
            resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        } catch (RuntimeException ex) {
            // Если биржа внезапно не принимает IOC — пробуем GTC
            String msg = ex.getMessage() != null ? ex.getMessage() : "";
            if (msg.toLowerCase().contains("timeinforce")) {
                log.warn("LIMIT BUY[AGGR] {}: биржа не приняла IOC, пробую GTC", symbol);
                params.put("timeInForce", "GTC");
                resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
            } else {
                throw ex;
            }
        }

        String orderId = resp.path("orderId").asText(null);
        String status  = resp.path("status").asText("UNKNOWN");

        BigDecimal executed = bd(resp.path("executedQty").asText("0"));
        BigDecimal cummQ    = bd(resp.path("cummulativeQuoteQty").asText("0"));
        BigDecimal avg      = safeAvg(cummQ, executed);

        log.info("📥 LIMIT BUY[AGGR] {}#{} result: status={}, executedQty={}, cummQuoteQty={}, avg={}",
                symbol, orderId, status, executed.toPlainString(), cummQ.toPlainString(), avg.toPlainString());

        // Если не финально — коротко подождём
        if (!"FILLED".equals(status) && !"CANCELED".equals(status) && !"REJECTED".equals(status)) {
            return waitUntilFilled(symbol, orderId, creds.getApiKey(), creds.getSecret(), 3000);
        }
        return new OrderInfo(orderId, status, executed, cummQ, avg);
    }

    /** Цена НАД спредом (для BUY): ask + N * tickSize, округление вниз к сетке (floor) */
    private BigDecimal priceAboveAsk(String symbol, int ticksAbove) {
        SymbolFilters f = getSymbolFilters(symbol);
        BookTicker t = fetchBookTicker(symbol);

        int n = Math.max(1, ticksAbove);
        BigDecimal raw = t.ask.add(f.tickSize.multiply(BigDecimal.valueOf(n)));

        // floor к сетке тика
        BigDecimal p = floorToStep(raw, f.tickSize);
        // гарантия, что действительно выше ask
        if (p.compareTo(t.ask) <= 0) {
            p = t.ask.add(f.tickSize);
            p = floorToStep(p, f.tickSize);
        }
        return normalizePrice(p, f);
    }

    /** Цена ПОД спредом (для SELL): bid - N * tickSize, округление вниз к сетке (floor) */
    private BigDecimal priceBelowBid(String symbol, int ticksBelow) {
        SymbolFilters f = getSymbolFilters(symbol);
        BookTicker t = fetchBookTicker(symbol);

        int n = Math.max(1, ticksBelow);
        BigDecimal raw = t.bid.subtract(f.tickSize.multiply(BigDecimal.valueOf(n)));

        // так как normalizePrice делает floor, для SELL «ниже bid» этого достаточно
        BigDecimal p = floorToStep(raw.max(f.tickSize), f.tickSize);
        // гарантия, что действительно ниже bid
        if (p.compareTo(t.bid) >= 0) {
            p = t.bid.subtract(f.tickSize).max(f.tickSize);
            p = floorToStep(p, f.tickSize);
        }
        return normalizePrice(p, f);
    }

    /** Мягкая попытка отмены спотового ордера (чтобы не словить дабл-покупку при фолбэке) */
    private void tryCancelOrder(String symbol, String orderId, String apiKey, String secret) {
        if (orderId == null) return;
        try {
            Map<String, String> p = new LinkedHashMap<>();
            p.put("symbol", symbol);
            p.put("orderId", orderId);
            signedRequest("DELETE", ORDER_ENDPOINT, p, apiKey, secret);
            log.warn("❌ Отменил зависший ордер {}#{} перед фолбэком", symbol, orderId);
        } catch (Exception ex) {
            log.warn("Не удалось отменить ордер {}#{}: {}", symbol, orderId, ex.getMessage());
        }
    }

}
