package com.suhoi.mexcdrainer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.Data;
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

    public final Map<String, CachedSymbolInfo> exchangeInfoCache = new ConcurrentHashMap<>();
    public static final class SymbolFilters {
        final BigDecimal tickSize;     // PRICE_FILTER.tickSize
        final BigDecimal stepSize;     // LOT_SIZE.stepSize
        final BigDecimal minQty;       // LOT_SIZE.minQty (опционально пригодится)
        final BigDecimal minNotional;  // MIN_NOTIONAL.minNotional (если биржа отдаёт)
        SymbolFilters(BigDecimal tickSize, BigDecimal stepSize, BigDecimal minQty, BigDecimal minNotional) {
            this.tickSize = tickSize != null ? tickSize : BigDecimal.ZERO;
            this.stepSize = stepSize != null ? stepSize : BigDecimal.ONE; // по умолчанию шаг 1 токен
            this.minQty = minQty != null ? minQty : BigDecimal.ZERO;
            this.minNotional = minNotional != null ? minNotional : BigDecimal.ZERO;
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

    public BigDecimal getTokenBalanceAccountB(String symbol, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
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

        // 1) Тянем фильтры и нормализуем входные price/qty
        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal normPrice = normalizePrice(price, f);
        BigDecimal normQty   = normalizeQty(qty, f);

        // 2) Базовые проверки
        if (normPrice.signum() <= 0 || normQty.signum() <= 0) {
            log.error("SELL {}: Невалидные параметры после нормализации: price={}, qty={}, rawPrice={}, rawQty={}",
                    symbol, normPrice.toPlainString(), normQty.toPlainString(),
                    price == null ? "null" : price.toPlainString(),
                    qty == null ? "null" : qty.toPlainString());
            return null;
        }

        // 3) Не нарушаем мин. нотионал (если биржа его отдаёт)
        if (!satisfiesNotional(normPrice, normQty, f)) {
            log.warn("SELL {}: Notional {} < minNotional {}. Ордер не отправлен. " +
                            "price={} qty={} (tickSize={} stepSize={} minQty={})",
                    symbol,
                    normPrice.multiply(normQty).stripTrailingZeros().toPlainString(),
                    f.minNotional.stripTrailingZeros().toPlainString(),
                    normPrice.toPlainString(), normQty.toPlainString(),
                    f.tickSize.stripTrailingZeros().toPlainString(),
                    f.stepSize.stripTrailingZeros().toPlainString(),
                    f.minQty.stripTrailingZeros().toPlainString());
            return null;
        }

        // 4) Логируем финальные значения до запроса
        log.info("SELL {}: финальные параметры -> price={} qty={} | (rawPrice={}, rawQty={}, tickSize={}, stepSize={}, minQty={}, minNotional={})",
                symbol,
                normPrice.toPlainString(), normQty.toPlainString(),
                price == null ? "null" : price.stripTrailingZeros().toPlainString(),
                qty == null ? "null" : qty.stripTrailingZeros().toPlainString(),
                f.tickSize.stripTrailingZeros().toPlainString(),
                f.stepSize.stripTrailingZeros().toPlainString(),
                f.minQty.stripTrailingZeros().toPlainString(),
                f.minNotional.stripTrailingZeros().toPlainString()
        );

        // 5) Формируем параметры без изменения структуры
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", normQty.toPlainString());   // ВАЖНО: уже кратно stepSize
        params.put("price",    normPrice.toPlainString()); // ВАЖНО: уже кратно tickSize

        JsonNode resp = signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
        if (resp != null && resp.has("orderId")) return resp.get("orderId").asText();
        log.warn("placeLimitSellAccountA unexpected response: {}", resp);
        return null;
    }


    public String placeLimitBuyAccountA(String symbol, BigDecimal price, BigDecimal usdtAmount, Long chatId) {
        var creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal normPrice = normalizePrice(price, f);

        // 1) Считаем сырое qty из бюджета: floor(usdt / price)
        BigDecimal rawQty;
        try {
            rawQty = usdtAmount.divide(normPrice, 18, RoundingMode.DOWN);
        } catch (ArithmeticException ex) {
            rawQty = BigDecimal.ZERO;
        }

        // 2) Нормализуем к stepSize
        BigDecimal qty = normalizeQty(rawQty, f);

        // 3) Проверки minQty / бюджет / minNotional
        // Если qty после нормализации стал ноль — пытаемся поднять до minQty, если бюджет позволяет
        if (qty.signum() <= 0 && f.minQty.signum() > 0) {
            BigDecimal minQtyToStep = floorToStep(f.minQty, f.stepSize);
            if (minQtyToStep.signum() <= 0) {
                // если minQty меньше шага — округлим вверх
                BigDecimal multiples = f.minQty.divide(f.stepSize, 0, RoundingMode.CEILING);
                minQtyToStep = multiples.multiply(f.stepSize);
            }
            // Проверяем бюджет
            if (minQtyToStep.multiply(normPrice).compareTo(usdtAmount) <= 0) {
                qty = minQtyToStep;
            }
        }

        // Не выходим за бюджет (после всех коррекций)
        BigDecimal cost = qty.multiply(normPrice);
        if (cost.compareTo(usdtAmount) > 0) {
            // урежем в рамках бюджета
            BigDecimal budgetQty = floorToStep(usdtAmount.divide(normPrice, 18, RoundingMode.DOWN), f.stepSize);
            qty = budgetQty;
            cost = qty.multiply(normPrice);
        }

        // Если есть MIN_NOTIONAL — обеспечим его (если возможно в рамках бюджета)
        if (f.minNotional.signum() > 0 && qty.signum() > 0 && cost.compareTo(f.minNotional) < 0) {
            // minimal qty для покрытия нотионала
            BigDecimal needQty = f.minNotional.divide(normPrice, 0, RoundingMode.CEILING).multiply(f.stepSize);
            // подогнать к сетке шага
            BigDecimal multiples = needQty.divide(f.stepSize, 0, RoundingMode.CEILING);
            needQty = multiples.multiply(f.stepSize);

            if (needQty.multiply(normPrice).compareTo(usdtAmount) <= 0) {
                qty = needQty;
                cost = qty.multiply(normPrice);
            } else {
                log.warn("BUY {}: невозможно удовлетворить minNotional={} в рамках бюджета {} USDT (price={}, stepSize={}). " +
                                "Расчётный cost={} — ордер не отправлен.",
                        symbol,
                        f.minNotional.stripTrailingZeros().toPlainString(),
                        usdtAmount.stripTrailingZeros().toPlainString(),
                        normPrice.toPlainString(),
                        f.stepSize.stripTrailingZeros().toPlainString(),
                        cost.stripTrailingZeros().toPlainString());
                return null;
            }
        }

        // Финальная валидация
        if (qty.signum() <= 0) {
            log.warn("placeLimitBuyAccountA: рассчитанное quantity <= 0 (usdt={}, price={}, stepSize={})",
                    usdtAmount.toPlainString(),
                    normPrice.toPlainString(),
                    f.stepSize.stripTrailingZeros().toPlainString());
            return null;
        }

        // Лог финальных параметров
        log.info("BUY {}: финальные параметры -> price={} qty={} (cost={}) | rawQty={} usdtBudget={} | tickSize={} stepSize={} minQty={} minNotional={}",
                symbol,
                normPrice.toPlainString(),
                qty.toPlainString(),
                cost.stripTrailingZeros().toPlainString(),
                rawQty.stripTrailingZeros().toPlainString(),
                usdtAmount.stripTrailingZeros().toPlainString(),
                f.tickSize.stripTrailingZeros().toPlainString(),
                f.stepSize.stripTrailingZeros().toPlainString(),
                f.minQty.stripTrailingZeros().toPlainString(),
                f.minNotional.stripTrailingZeros().toPlainString()
        );

        // 4) Формируем параметры без изменения структуры
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", qty.toPlainString());        // кратно stepSize
        params.put("price",    normPrice.toPlainString());  // кратно tickSize

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
        params.put("quantity", qty.setScale(5, RoundingMode.DOWN).toPlainString());
        params.put("price", price.stripTrailingZeros().toPlainString());

        signedRequest("POST", ORDER_ENDPOINT, params, apiKey, secret);
    }

    public void marketBuyFromAccountB(String symbol, BigDecimal price, BigDecimal qty, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        // сколько примерно нужно USDT чтобы купить qty токенов
        BigDecimal requiredUsdt = BigDecimal.ZERO;
        try {
            requiredUsdt = price.multiply(qty).setScale(5, RoundingMode.UP);
        } catch (Exception ex) {
            log.warn("Ошибка расчёта requiredUsdt: {}", ex.getMessage(), ex);
        }

        // сколько реально есть USDT на аккаунте B
        BigDecimal availableUsdt = getAssetBalance(creds.getApiKey(), creds.getSecret(), "USDT");
        if (availableUsdt.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("B не имеет USDT для покупки (available=0)");
            return;
        }

        // если мало USDT — уменьшаем qty до максимально возможного
        if (availableUsdt.compareTo(requiredUsdt) < 0) {
            BigDecimal adjustedQty = availableUsdt.divide(price, 8, RoundingMode.DOWN);
            if (adjustedQty.compareTo(BigDecimal.ZERO) <= 0) {
                log.warn("B недостаточно USDT ({}) чтобы купить хоть немного токенов по цене {}", availableUsdt, price);
                return;
            }
            log.info("B имеет меньше USDT ({}) чем нужно ({}). Уменьшаем qty -> {}", availableUsdt, requiredUsdt, adjustedQty);
            qty = adjustedQty;
            requiredUsdt = availableUsdt;
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "BUY");
        params.put("type", "MARKET");
        // для MARKET BUY используем quoteOrderQty (сколько USDT потратить)
        params.put("quoteOrderQty", requiredUsdt.toPlainString());

        signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
    }

    // Исправленный marketSellFromAccountB — продаёт qty токенов (если B имеет меньше — уменьшаем qty).
    // ⬇️ Заменить целиком
    public void marketSellFromAccountB(String symbol, BigDecimal price, BigDecimal qty, Long chatId) {
        var creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        // Для MARKET SELL передаём quantity (базовый ассет). price используем только для оценки notional.
        SymbolFilters f = getSymbolFilters(symbol);

        String asset = symbol.endsWith("USDT") ? symbol.substring(0, symbol.length() - 4) : symbol;
        BigDecimal available = getAssetBalance(creds.getApiKey(), creds.getSecret(), asset);

        if (available.signum() <= 0) {
            log.warn("MARKET SELL {}: у B нет доступных токенов ({})", symbol, asset);
            return;
        }

        // Не продаём больше, чем есть
        BigDecimal requested = (qty == null) ? BigDecimal.ZERO : qty;
        BigDecimal capped = requested.compareTo(available) <= 0 ? requested : available;

        // Нормализуем к stepSize (строго вниз)
        BigDecimal normQty = normalizeQty(capped, f);

        // Проверим minNotional, если задан, при наличии адекватной оценочной цены
        if (f.minNotional.signum() > 0 && price != null && price.signum() > 0) {
            BigDecimal estNotional = price.multiply(normQty);
            if (estNotional.compareTo(f.minNotional) < 0) {
                // Сколько нужно продать минимум при этой цене
                BigDecimal needQty = f.minNotional
                        .divide(price, 0, RoundingMode.CEILING)  // минимум штук
                        .multiply(f.stepSize);                    // на сетку шага
                // подогнать к сетке
                BigDecimal multiples = needQty.divide(f.stepSize, 0, RoundingMode.CEILING);
                needQty = multiples.multiply(f.stepSize);

                if (needQty.compareTo(available) <= 0) {
                    normQty = needQty;
                    estNotional = price.multiply(normQty);
                } else {
                    log.warn("MARKET SELL {}: minNotional={} не покрывается: доступно {} {}, требуется qty={} при price={}. Ордер не отправлен.",
                            symbol,
                            f.minNotional.stripTrailingZeros().toPlainString(),
                            available.stripTrailingZeros().toPlainString(), asset,
                            needQty.stripTrailingZeros().toPlainString(),
                            price.stripTrailingZeros().toPlainString());
                    return;
                }
            }
        }

        if (normQty.signum() <= 0) {
            log.warn("MARKET SELL {}: рассчитанное qty <= 0 (requested={}, available={}, stepSize={})",
                    symbol,
                    requested.stripTrailingZeros().toPlainString(),
                    available.stripTrailingZeros().toPlainString(),
                    f.stepSize.stripTrailingZeros().toPlainString());
            return;
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

        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", "SELL");
        params.put("type", "MARKET");
        // ВАЖНО: НИКАКИХ setScale(5)! Только кратность stepSize и "чистая" строка.
        params.put("quantity", normQty.toPlainString());

        signedRequest("POST", ORDER_ENDPOINT, params, creds.getApiKey(), creds.getSecret());
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
            // 1) Берём стакан
            String url = API_BASE + TICKER_BOOK + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode resp = objectMapper.readTree(body);

            BigDecimal bid = new BigDecimal(resp.path("bidPrice").asText("0"));
            BigDecimal ask = new BigDecimal(resp.path("askPrice").asText("0"));

            // 2) Фолбэки от "мертвого" стакана
            if (bid.signum() <= 0 && ask.signum() > 0) {
                bid = ask; // нет бидов — поставим равным аску, спред = 0
            } else if (ask.signum() <= 0 && bid.signum() > 0) {
                ask = bid; // нет асков
            } else if (bid.signum() <= 0 && ask.signum() <= 0) {
                // совсем пусто — вернём минимальную ненулевую цену (один тик)
                SymbolFilters f = getSymbolFilters(symbol);
                BigDecimal p = f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
                log.warn("Пустой стакан {} — возвращаю 1 тик: {}", symbol, p);
                return p;
            }

            BigDecimal spread = ask.subtract(bid);
            if (spread.signum() < 0) spread = BigDecimal.ZERO;

            // 3) Наша целевая «около нижней границы»: bid + 10% спреда
            BigDecimal raw = bid.add(spread.multiply(new BigDecimal("0.10")));

            // 4) Нормализуем к тиксайзу (чтобы не получить 0 и несловм исходный scale)
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
            // Фолбэк — хотя бы не 0
            SymbolFilters f = getSymbolFilters(symbol);
            return f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
        }
    }


    public BigDecimal getNearUpperSpreadPrice(String symbol) {
        try {
            // 1) Берём стакан
            String url = API_BASE + TICKER_BOOK + "?symbol=" + symbol;
            String body = restTemplate.getForObject(url, String.class);
            JsonNode resp = objectMapper.readTree(body);

            BigDecimal bid = new BigDecimal(resp.path("bidPrice").asText("0"));
            BigDecimal ask = new BigDecimal(resp.path("askPrice").asText("0"));

            // 2) Фолбэки на пустой стакан
            if (bid.signum() <= 0 && ask.signum() > 0) {
                bid = ask;
            } else if (ask.signum() <= 0 && bid.signum() > 0) {
                ask = bid;
            } else if (bid.signum() <= 0 && ask.signum() <= 0) {
                SymbolFilters f = getSymbolFilters(symbol);
                BigDecimal p = f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
                log.warn("Пустой стакан {} — возвращаю 1 тик (upper): {}", symbol, p);
                return p;
            }

            BigDecimal spread = ask.subtract(bid);
            if (spread.signum() < 0) spread = BigDecimal.ZERO;

            // 3) Цель возле верхней границы: ask - 10% спреда
            BigDecimal raw = ask.subtract(spread.multiply(new BigDecimal("0.10")));

            // 4) Нормализуем к тиксайзу
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

    private JsonNode signedRequestTest(String method, String path, Map<String, String> params, String apiKey, String secret) {
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

    /**
     * Округляет value ВНИЗ до ближайшего кратного step (floor к сетке).
     * Работает корректно даже при очень малых шагах (1e-9 и т.д.).
     */
    private static BigDecimal floorToStep(BigDecimal value, BigDecimal step) {
        if (value == null || step == null || step.signum() <= 0) return value;
        if (value.signum() <= 0) return BigDecimal.ZERO;

        // value = floor(value / step) * step
        BigDecimal multiples = value.divide(step, 0, RoundingMode.DOWN);
        return multiples.multiply(step);
    }

    /** Корректирует и валидирует цену (не даём уйти в 0). */
    private static BigDecimal normalizePrice(BigDecimal rawPrice, SymbolFilters f) {
        BigDecimal p = floorToStep(rawPrice, f.tickSize);
        if (p == null || p.signum() <= 0) {
            // fallback: минимально допустимая положительная цена = один тик
            p = f.tickSize.signum() > 0 ? f.tickSize : new BigDecimal("0.00000001");
        }
        return p.stripTrailingZeros();
    }

    /** Корректирует и валидирует количество (до кратности stepSize). */
    private static BigDecimal normalizeQty(BigDecimal rawQty, SymbolFilters f) {
        BigDecimal q = floorToStep(rawQty, f.stepSize);
        if (q == null) q = BigDecimal.ZERO;

        // не даём уйти ниже minQty, если оно задано и у нас достаточно сырого объёма
        if (f.minQty.signum() > 0 && q.signum() > 0 && q.compareTo(f.minQty) < 0) {
            // попробуем подтянуть до minQty вверх к кратности stepSize
            BigDecimal neededMultiples = f.minQty.divide(f.stepSize, 0, RoundingMode.CEILING);
            q = neededMultiples.multiply(f.stepSize);
            // если подняли больше, чем есть реально на балансе — всё равно поставим вниз (q может стать 0)
            if (q.compareTo(rawQty) > 0) {
                q = floorToStep(rawQty, f.stepSize);
            }
        }

        return q.stripTrailingZeros();
    }

    /** Грубо проверяем notional, если фильтр присутствует (может отсутствовать на MEXC). */
    private static boolean satisfiesNotional(BigDecimal price, BigDecimal qty, SymbolFilters f) {
        if (f.minNotional.signum() <= 0) return true;
        if (price == null || qty == null) return false;
        BigDecimal notional = price.multiply(qty);
        return notional.compareTo(f.minNotional) >= 0;
    }

    /** Достаём фильтры символа из кэша/сети. */
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

            // структура как у Binance: symbols[0].filters[...]
            JsonNode symbols = json.get("symbols");
            if (symbols == null || !symbols.isArray() || symbols.isEmpty()) {
                log.warn("exchangeInfo: пустой symbols для {}", symbol);
                // дефолтные шаги — лучше, чем ничего
                SymbolFilters def = new SymbolFilters(new BigDecimal("0.00000001"), BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ZERO);
                exchangeInfoCache.put(symbol, new CachedSymbolInfo(def, now));
                return def;
            }

            JsonNode s0 = symbols.get(0);
            BigDecimal tickSize = null;
            BigDecimal stepSize = null;
            BigDecimal minQty   = null;
            BigDecimal minNotional = null;

            JsonNode filters = s0.get("filters");
            if (filters != null && filters.isArray()) {
                for (JsonNode f : filters) {
                    String type = f.path("filterType").asText("");
                    switch (type) {
                        case "PRICE_FILTER" -> {
                            tickSize = new BigDecimal(f.path("tickSize").asText("0.00000001"));
                        }
                        case "LOT_SIZE" -> {
                            stepSize = new BigDecimal(f.path("stepSize").asText("1"));
                            minQty   = new BigDecimal(f.path("minQty").asText("0"));
                        }
                        case "MIN_NOTIONAL", "NOTIONAL" -> {
                            // встречается разный нейминг на совместимых API
                            String v = f.has("minNotional") ? f.path("minNotional").asText("0")
                                    : f.has("minNotionalValue") ? f.path("minNotionalValue").asText("0")
                                    : f.path("minNotional").asText("0");
                            minNotional = new BigDecimal(v);
                        }
                        default -> { /* ignore */ }
                    }
                }
            }

            SymbolFilters parsed = new SymbolFilters(
                    tickSize != null ? tickSize : new BigDecimal("0.00000001"),
                    stepSize != null ? stepSize : BigDecimal.ONE,
                    minQty   != null ? minQty   : BigDecimal.ZERO,
                    minNotional != null ? minNotional : BigDecimal.ZERO
            );

            exchangeInfoCache.put(symbol, new CachedSymbolInfo(parsed, now));

            log.info("exchangeInfo[{}]: tickSize={}, stepSize={}, minQty={}, minNotional={}",
                    symbol, parsed.tickSize, parsed.stepSize, parsed.minQty, parsed.minNotional);

            return parsed;
        } catch (Exception e) {
            log.error("Ошибка exchangeInfo для {}: {}", symbol, e.getMessage(), e);
            // Фолбэк — очень консервативные шаги
            SymbolFilters def = new SymbolFilters(new BigDecimal("0.00000001"), BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ZERO);
            exchangeInfoCache.put(symbol, new CachedSymbolInfo(def, now));
            return def;
        }
    }



}
