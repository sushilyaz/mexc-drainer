package com.suhoi.mexcdrainer.ws.facade;

import com.suhoi.mexcdrainer.service.MexcTradeService;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

@Slf4j
@RequiredArgsConstructor
public class WsEnsureFacade {

    private final MarketBookFacade book;
    private final MexcTradeService mexc;

    public record RequoteResult(boolean ok, String reason, String orderId, BigDecimal price, int requotes) {}

    /**
     * ENSURE для A-SELL: убеждаемся, что наш SELL — на инклюзивном top ask.
     * Если нас подрезали — переставляемся ниже инклюзивного top ask на epsilonTicks.
     */
    public RequoteResult ensureTopAskOrRequoteSellWs(
            String symbol, Long chatId,
            String currentOrderId, BigDecimal currentPrice, BigDecimal qty,
            int maxRequotes, int epsilonTicks, int postPlaceGraceMs
    ) {
        if (currentOrderId == null || currentPrice == null) {
            return new RequoteResult(false, "BAD_ARGS", currentOrderId, currentPrice, 0);
        }
        final int maxRq = Math.max(0, maxRequotes);
        final int grace = Math.max(10, Math.min(postPlaceGraceMs, 500));

        var f = mexc.getSymbolFilters(symbol);
        BigDecimal tick = (f != null && f.getTickSize() != null && f.getTickSize().signum() > 0)
                ? f.getTickSize() : new BigDecimal("0.00000001");

        String orderId = currentOrderId;
        BigDecimal price = currentPrice;

        for (int i = 0; i <= maxRq; ) {
            sleep(grace);

            // сравниваем с ИНКЛЮЗИВНЫМ top ask, иначе будем бесконечно «накалывать» сами себя
            var topInc = book.topInclusive(symbol);
            if (topInc.isEmpty()) {
                log.warn("[ENSURE_A_SELL] book unavailable (stale/resync), skip requote");
                return new RequoteResult(true, "BOOK_UNAVAILABLE", orderId, price, i);
            }
            BigDecimal topAsk = topInc.get().ask();

            // лаг книги — наш ордер не «проклеился» (topAsk > наша цена)
            if (topAsk.compareTo(price) > 0) {
                sleep(Math.min(grace, 60));
                continue;
            }
            // OK: наша цена == инклюзивный top ask
            int dticks = ticksBetween(price, topAsk, tick);
            if (dticks == 0) {
                log.info("[ENSURE_A_SELL] OK top ask price={} topAsk={}", price, topAsk);
                return new RequoteResult(true, "OK", orderId, price, i);
            }

            // Нас подрезали → переставляемся ниже topAsk на epsilonTicks
            BigDecimal newPrice = topAsk.subtract(tick.multiply(BigDecimal.valueOf(Math.max(1, epsilonTicks))));
            newPrice = mexc.alignPriceCeil(symbol, newPrice.max(tick));

            // перестановка: cancel + place
            mexc.cancelOrderAccountA(symbol, orderId, chatId);
            String newOrderId = mexc.placeLimitSellAccountA(symbol, newPrice, qty, chatId);

            log.warn("🔁 [ENSURE_A_SELL] REQUOTE {} -> {} ({} -> {}) | inc.topAsk={} ε={}",
                    price.stripTrailingZeros(), newPrice.stripTrailingZeros(),
                    orderId, newOrderId, topAsk.stripTrailingZeros(), epsilonTicks);

            orderId = newOrderId;
            price   = newPrice;
            i++;
        }
        return new RequoteResult(false, "LIMIT_REACHED", orderId, price, maxRq);
    }

    private static int ticksBetween(BigDecimal a, BigDecimal b, BigDecimal tick) {
        if (a == null || b == null || tick == null || tick.signum() <= 0) return Integer.MAX_VALUE;
        BigDecimal diff = a.subtract(b).abs();
        return diff.divide(tick, 0, BigDecimal.ROUND_HALF_UP).intValue();
    }

    private static void sleep(int ms) { try { Thread.sleep(ms); } catch (InterruptedException ignored) {} }
}
