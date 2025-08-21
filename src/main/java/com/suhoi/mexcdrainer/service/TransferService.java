package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.config.MexcClient;
import com.suhoi.mexcdrainer.dto.*;
import com.suhoi.mexcdrainer.telegram.KeyStore.Keys;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransferService {

    private final MexcClient mexc;
    private final AppProperties props;

    // Безопасный дефолт минимального нотионала на MEXC (часто 1 USDT)
    private static final BigDecimal MIN_NOTIONAL_QUOTE = BigDecimal.ONE;

    public String drainAll(long chatId, String symbol, Keys a, Keys b) {
        ExchangeInfoResponse ex = mexc.exchangeInfo(symbol);
        SymbolInfo info = ex.getSymbols().stream()
                .filter(s -> symbol.equalsIgnoreCase(s.getSymbol()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Неизвестный символ: " + symbol));

        final String base = info.getBaseAsset();   // напр. DAOLITY
        final String quote = info.getQuoteAsset(); // USDT

        final int priceScale = info.getQuotePrecision(); // знаков в цене
        final int qtyScale   = deriveQtyScale(info);     // знаков в количестве (наш расчёт)
        final BigDecimal baseStep = BigDecimal.ONE.movePointLeft(Math.max(qtyScale, 0));

        // минимальная сумма рыночной покупки по квоте — если нет, дефолт 1
        final BigDecimal minMarketQuote = Optional.ofNullable(info.getQuoteAmountPrecisionMarket())
                .map(TransferService::parseOrZero).filter(v -> v.compareTo(BigDecimal.ZERO) > 0)
                .orElse(MIN_NOTIONAL_QUOTE);

        int cycle = 0;
        BigDecimal totalUsdtMoved = BigDecimal.ZERO;

        while (cycle < props.getMexc().getMaxCycles()) {
            cycle++;

            // Балансы
            AccountInfo accA = mexc.account(a.getApiKey(), a.getSecret());
            AccountInfo accB = mexc.account(b.getApiKey(), b.getSecret());

            BigDecimal aUsdt = free(accA, quote);
            BigDecimal aBase = free(accA, base);
            BigDecimal bUsdt = free(accB, quote);
            BigDecimal bBase = free(accB, base);

            log.info("▶️ Старт цикла {} по {}", cycle, symbol);
            log.info("ℹ️ Балансы: A.{}={}  A.{}={}  |  B.{}={}  B.{}={}",
                    quote, aUsdt, base, aBase, quote, bUsdt, base, bBase);

            // Актуальный спред
            BookTicker t0 = mexc.bookTicker(symbol);
            BigDecimal bestBid0 = parseOrZero(t0.getBidPrice());
            BigDecimal bestAsk0 = parseOrZero(t0.getAskPrice());

            // Решаем, стартовать ли с SELL
            boolean haveEnoughBaseQty = aBase.compareTo(baseStep) >= 0;
            boolean haveEnoughBaseNotional = bestBid0.multiply(aBase).setScale(priceScale, RoundingMode.DOWN)
                    .compareTo(MIN_NOTIONAL_QUOTE) >= 0;
            boolean startWithSell = haveEnoughBaseQty && haveEnoughBaseNotional;

            if (!startWithSell && aUsdt.compareTo(minMarketQuote) < 0) {
                log.info("⛔ На A нет достаточного {} для MARKET BUY ({}<{}) и нотионал {} слишком мал ({}<{}) — останавливаю.",
                        quote, aUsdt, minMarketQuote, base, bestBid0.multiply(aBase), MIN_NOTIONAL_QUOTE);
                break;
            }

            // ==== [1] Если есть валидный базовый остаток — сразу SELL внутри спреда, иначе MARKET BUY
            if (!startWithSell) {
                BigDecimal safety = bd(props.getMexc().getSafetySpendPct()); // напр. 0.995
                BigDecimal spend = aUsdt.multiply(safety);

                log.info("🛒 A: покупаю по рынку {} на сумму ≈ {} {} (safety={})", base, spend, quote, safety);
                log.info("➡️ Перед MARKET BUY: свободно {}={}, покупаю на {}", quote, aUsdt, spend);

                OrderResponse r1 = mexc.newOrder(a.getApiKey(), a.getSecret(), NewOrderRequest.builder()
                        .symbol(symbol).side("BUY").type("MARKET")
                        .quoteOrderQty(fmt(spend, priceScale))
                        .newClientOrderId("A_MBUY_" + System.currentTimeMillis())
                        .build());
                log.info("✅ A: MARKET BUY исполнен, orderId={}", r1.getOrderId());

                // обновим баланс A
                accA = mexc.account(a.getApiKey(), a.getSecret());
                aBase = free(accA, base);
                if (aBase.compareTo(baseStep) < 0) {
                    log.info("⛔ После покупки на A {} слишком мало для ордера: {} (шаг {})", base, aBase, baseStep);
                    break;
                }
            } else {
                log.info("♻️ На A уже есть валидный остаток {}={} (step={}, notional@bid={}). Начинаю с SELL внутри спреда.",
                        base, aBase, baseStep, bestBid0.multiply(aBase));
            }

            // ==== [2] A: SELL-LIMIT ВНУТРИ СПРЕДА (у нижней границы)
            InsidePlan askPlan = insideSellPlan(symbol, priceScale);
            BigDecimal sellQty = floorToStep(aBase, baseStep);

            // Прикидываем нотионал по стартовой цене
            BigDecimal sellNotional = askPlan.startPrice.multiply(sellQty).setScale(priceScale, RoundingMode.DOWN);
            if (sellNotional.compareTo(MIN_NOTIONAL_QUOTE) < 0) {
                log.info("⛔ Нотионал SELL слишком мал: {} {} (< {} {}), qty={}, price={}. Пропускаю SELL.",
                        sellNotional, quote, MIN_NOTIONAL_QUOTE, quote, sellQty, askPlan.startPrice);
            } else {
                log.info("➡️ Готовлю A: SELL внутри спреда qty={} {} на цене старт={}", sellQty, base, askPlan.startPrice);
                OrderResponse sellOrder = placeUniqueLimitInsideSpread(
                        a.getApiKey(), a.getSecret(), symbol, "SELL", sellQty, qtyToScale(sellQty), askPlan, priceScale, true);

                BigDecimal sellQtyLeft = remainingFor(a.getApiKey(), a.getSecret(), symbol, sellOrder.getOrderId(), sellQty);
                BigDecimal sellPrice   = parseOrZero(sellOrder.getPrice());
                log.info("📤 A: выставил SELL внутри спреда qty={} {} по цене {}. Остаток кросса={} {}",
                        sellQty, base, sellPrice, sellQtyLeft, base);

                // ==== [3] B: BUY — выкупаем остаток лимитки A
                if (sellQtyLeft.compareTo(BigDecimal.ZERO) > 0) {
                    log.info("🤝 B: выкупаю лимитку A: qty={} {} по цене {}", sellQtyLeft, base, sellPrice);
                    OrderResponse bBuyResp = mexc.newOrder(b.getApiKey(), b.getSecret(), NewOrderRequest.builder()
                            .symbol(symbol).side("BUY").type("LIMIT")
                            .quantity(fmt(sellQtyLeft, qtyToScale(sellQtyLeft)))
                            .price(fmt(sellPrice, priceScale))
                            .newClientOrderId("B_BUY_" + System.currentTimeMillis())
                            .build());
                    log.info("✅ B: BUY исполнен/принят, orderId={}", bBuyResp.getOrderId());
                } else {
                    log.info("ℹ️ SELL-лимитка A уже исполнена полностью — шаг с B пропускаю");
                }
            }

            // ==== [4] A: BUY-LIMIT ВНУТРИ СПРЕДА (у верхней границы)
            InsidePlan bidPlan = insideBuyPlan(symbol, priceScale);
            accA = mexc.account(a.getApiKey(), a.getSecret());
            BigDecimal aUsdtAfterSell = free(accA, quote);

            // бюджет c safety
            BigDecimal safety = bd(props.getMexc().getSafetySpendPct());
            BigDecimal buyPrice = bidPlan.startPrice;

            BigDecimal buyBudget = aUsdtAfterSell.multiply(safety);
            BigDecimal buyQtyRaw = (buyPrice.compareTo(BigDecimal.ZERO) > 0)
                    ? buyBudget.divide(buyPrice, qtyScale + 6, RoundingMode.DOWN)
                    : BigDecimal.ZERO;
            BigDecimal buyQty = floorToStep(buyQtyRaw, baseStep);
            buyQty = fitQtyUnderQuote(aUsdtAfterSell, buyPrice, buyQty, baseStep, priceScale);

            // Нотионал BUY
            BigDecimal reqQuote = buyPrice.multiply(buyQty).setScale(priceScale, RoundingMode.DOWN);
            if (reqQuote.compareTo(MIN_NOTIONAL_QUOTE) < 0) {
                log.info("⛔ Нотионал BUY слишком мал: {} {} (< {} {}), qty={}, price={}. Пропускаю BUY.",
                        reqQuote, quote, MIN_NOTIONAL_QUOTE, quote, buyQty, buyPrice);
            } else {
                log.info("➡️ Готовлю A: BUY внутри спреда. Свободно {}={} | Потребуется ≈ {} {} при цене {}. " +
                                "budget={} {}, qty={} {} (step={})",
                        quote, aUsdtAfterSell, reqQuote, quote, buyPrice, buyBudget, quote, buyQty, base, baseStep);

                OrderResponse buyOrder = placeUniqueLimitInsideSpread(
                        a.getApiKey(), a.getSecret(), symbol, "BUY", buyQty, qtyToScale(buyQty), bidPlan, priceScale, false);

                BigDecimal aBuyPrice   = parseOrZero(buyOrder.getPrice());
                BigDecimal aBuyQtyLeft = remainingFor(a.getApiKey(), a.getSecret(), symbol, buyOrder.getOrderId(), buyQty);
                log.info("📥 A: выставил BUY внутри спреда qty={} {} по цене {}. Остаток кросса={} {}",
                        buyQty, base, aBuyPrice, aBuyQtyLeft, base);

                // ==== [5] B: MARKET SELL в BUY-лимит A
                if (aBuyQtyLeft.compareTo(BigDecimal.ZERO) > 0) {
                    accB = mexc.account(b.getApiKey(), b.getSecret());
                    bBase = free(accB, base);
                    BigDecimal sellMarketQty = aBuyQtyLeft.min(floorToStep(bBase, baseStep));

                    if (sellMarketQty.compareTo(BigDecimal.ZERO) > 0) {
                        log.info("💸 B: продаю по рынку в BUY A: qty={} {} (у B free={} {})",
                                sellMarketQty, base, bBase, base);
                        log.info("➡️ Перед MARKET SELL B: qty={} {}.", sellMarketQty, base);

                        OrderResponse bSellResp = mexc.newOrder(b.getApiKey(), b.getSecret(), NewOrderRequest.builder()
                                .symbol(symbol).side("SELL").type("MARKET")
                                .quantity(fmt(sellMarketQty, qtyToScale(sellMarketQty)))
                                .newClientOrderId("B_SELL_MKT_" + System.currentTimeMillis())
                                .build());
                        log.info("✅ B: MARKET SELL отправлен/исполнен, orderId={}", bSellResp.getOrderId());
                    } else {
                        log.info("ℹ️ У B нет базового актива для рыночной продажи. Шаг с B пропускаю.");
                    }

                    // учет перелитого (грубая оценка)
                    BigDecimal movedNow = aBuyPrice.multiply(aBuyQtyLeft);
                    totalUsdtMoved = totalUsdtMoved.add(movedNow);
                    log.info("📦 Перелито за цикл ≈ {} {} (через покупку A по цене {})", movedNow, quote, aBuyPrice);
                } else {
                    log.info("ℹ️ BUY-лимитка A уже исполнена полностью — рыночная продажа B не требуется");
                }
            }

            // Телеметрия по концу цикла
            accA = mexc.account(a.getApiKey(), a.getSecret());
            log.info("🔁 Итог цикла {}: A.{}≈{}  A.{}≈{}", cycle, quote, free(accA, quote), base, free(accA, base));
        }

        log.info("🏁 Завершение drain: циклов={}, суммарно перелито≈{} {}", cycle, totalUsdtMoved, quote);
        return "cycles=" + cycle + ", moved≈" + totalUsdtMoved + " " + quote;
    }

    // ===================== ВЫБОР ЦЕН ВНУТРИ СПРЕДА =====================

    /** План внутри спреда. */
    record InsidePlan(BigDecimal startPrice, BigDecimal tick, BigDecimal minInside, BigDecimal maxInside) {}

    /** SELL: bestBid + tick .. ask - tick (или ask, если спред = 1 тик). */
    private InsidePlan insideSellPlan(String symbol, int priceScale) {
        BookTicker t = mexc.bookTicker(symbol);
        BigDecimal bid = parseOrZero(t.getBidPrice());
        BigDecimal ask = parseOrZero(t.getAskPrice());
        BigDecimal tick = tickFromBook(symbol);

        BigDecimal minInside = bid.add(tick).setScale(priceScale, RoundingMode.DOWN);
        BigDecimal regularMaxInside = ask.subtract(tick).setScale(priceScale, RoundingMode.DOWN);
        BigDecimal maxInside = (regularMaxInside.compareTo(minInside) >= 0) ? regularMaxInside : ask;
        BigDecimal start = minInside.min(maxInside);
        return new InsidePlan(start, tick, minInside, maxInside);
    }

    /** BUY: ask - tick .. bid + tick (или bid, если спред = 1 тик). */
    private InsidePlan insideBuyPlan(String symbol, int priceScale) {
        BookTicker t = mexc.bookTicker(symbol);
        BigDecimal bid = parseOrZero(t.getBidPrice());
        BigDecimal ask = parseOrZero(t.getAskPrice());
        BigDecimal tick = tickFromBook(symbol);

        BigDecimal maxInside = ask.subtract(tick).setScale(priceScale, RoundingMode.DOWN);
        BigDecimal regularMinInside = bid.add(tick).setScale(priceScale, RoundingMode.DOWN);
        BigDecimal minInside = (regularMinInside.compareTo(maxInside) <= 0) ? regularMinInside : bid;
        BigDecimal start = maxInside.max(minInside);
        return new InsidePlan(start, tick, minInside, maxInside);
    }

    // ===================== ПОСТАНОВКА И ПЕРЕСТАНОВКА ВНУТРИ СПРЕДА =====================

    private OrderResponse placeUniqueLimitInsideSpread(
            String apiKey, String secret, String symbol, String side,
            BigDecimal qty, int qtyScaleForThisQty, InsidePlan plan, int priceScale, boolean askSide) {

        int reposts = 0;
        BigDecimal currentPrice = plan.startPrice;

        while (true) {
            // ЛОГ до отправки
            BigDecimal reqQuoteLog = askSide ? BigDecimal.ZERO
                    : currentPrice.multiply(qty).setScale(priceScale, RoundingMode.DOWN);
            log.info("➡️ Перед отправкой {}(inside): qty={} по цене {}. Требуется ≈ {} {}",
                    side, qty, currentPrice, reqQuoteLog, askSide ? "" : "USDT");

            OrderResponse placed = mexc.newOrder(apiKey, secret, NewOrderRequest.builder()
                    .symbol(symbol).side(side).type("LIMIT")
                    .quantity(fmt(qty, qtyScaleForThisQty))
                    .price(fmt(currentPrice, priceScale))
                    .newClientOrderId(side.charAt(0) + "_LIM_" + System.currentTimeMillis())
                    .build());
            String orderId = placed.getOrderId();

            log.info("📌 {}(inside): поставил лимитку orderId={} qty={} по цене {}", side, orderId, qty, currentPrice);

            // Мониторинг «только мой»
            while (true) {
                try { Thread.sleep(props.getMexc().getPollMs()); } catch (InterruptedException ignored) {}

                var openOpt = mexc.openOrders(apiKey, secret, symbol).stream()
                        .filter(o -> orderId.equals(o.getOrderId()))
                        .findFirst();
                if (openOpt.isEmpty()) {
                    log.info("✅ Лимитка orderId={} исполнилась/снята — продолжаю", orderId);
                    return placed;
                }

                OpenOrder open = openOpt.get();
                BigDecimal myRemain = parseOrZero(open.getOrigQty()).subtract(parseOrZero(open.getExecutedQty()));

                Depth d = mexc.depth(symbol, 20);
                BigDecimal levelQty = side.equals("SELL")
                        ? levelQty(d.getAsks(), currentPrice)
                        : levelQty(d.getBids(), currentPrice);

                boolean onlyMine = levelQty.compareTo(myRemain) == 0;
                if (onlyMine) {
                    log.info("🧭 На котировке {} только мой объём {} — оставляю.", currentPrice, myRemain);
                    return placed;
                }

                // Подсадка — двигаем к центру
                BookTicker t = mexc.bookTicker(symbol);
                BigDecimal bid = parseOrZero(t.getBidPrice());
                BigDecimal ask = parseOrZero(t.getAskPrice());
                BigDecimal tick = plan.tick;

                BigDecimal minInside = bid.add(tick).setScale(priceScale, RoundingMode.DOWN);
                BigDecimal maxInside = ask.subtract(tick).setScale(priceScale, RoundingMode.DOWN);
                if (maxInside.compareTo(minInside) < 0) {
                    if (askSide) { maxInside = ask; minInside = bid.add(tick).min(maxInside); }
                    else { minInside = bid; maxInside = ask.subtract(tick).max(minInside); }
                }

                BigDecimal next = askSide ? currentPrice.add(tick) : currentPrice.subtract(tick);
                if (next.compareTo(minInside) < 0) next = minInside;
                if (next.compareTo(maxInside) > 0) next = maxInside;

                if (next.compareTo(currentPrice) == 0) {
                    log.info("ℹ️ Нет возможности переставить {} внутри спреда (граница). Оставляю {}.", side, currentPrice);
                    return placed;
                }

                log.info("⚠️ {}: на уровне {} подсадили чужой объём (уровень={}, мой остаток={}). Переставляю: {} → {}",
                        side, currentPrice, levelQty, myRemain, currentPrice, next);

                mexc.cancelOrder(apiKey, secret, symbol, orderId);
                currentPrice = next;
                reposts++;

                if (reposts > props.getMexc().getMaxReposts()) {
                    throw new IllegalStateException("Слишком много перестановок по " + side + " " + symbol);
                }

                break; // снова поставим и продолжим мониторинг
            }
        }
    }

    // ===================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ =====================

    private static int deriveQtyScale(SymbolInfo info) {
        // 1) если baseSizePrecision выглядит как шаг (например "0.001") — берём её точность
        String step = info.getBaseSizePrecision();
        if (step != null && step.contains(".")) {
            return scaleFromStepString(step);
        }

        // 2) иначе используем baseAssetPrecision (это количество знаков в количестве базового актива)
        int p = info.getBaseAssetPrecision();
        if (p >= 0) return p;

        // 3) дефолт
        return 8;
    }


    private static Integer nullSafeInt(Integer v) { return v; }

    private static BigDecimal levelQty(List<String[]> side, BigDecimal price) {
        for (var lvl : side) if (parseOrZero(lvl[0]).compareTo(price) == 0) return parseOrZero(lvl[1]);
        return BigDecimal.ZERO;
    }

    private static BigDecimal free(AccountInfo acc, String asset) {
        return acc.getBalances().stream()
                .filter(b -> asset.equals(b.getAsset()))
                .map(b -> parseOrZero(b.getFree()))
                .findFirst().orElse(BigDecimal.ZERO);
    }

    private BigDecimal remainingFor(String apiKey, String secret, String symbol, String orderId, BigDecimal fallbackQty) {
        return mexc.openOrders(apiKey, secret, symbol).stream()
                .filter(o -> orderId.equals(o.getOrderId()))
                .findFirst()
                .map(o -> {
                    BigDecimal orig = parseOrZero(o.getOrigQty());
                    BigDecimal exec = parseOrZero(o.getExecutedQty());
                    if (orig.signum() == 0 && exec.signum() == 0) return fallbackQty;
                    BigDecimal left = orig.subtract(exec);
                    return left.max(BigDecimal.ZERO);
                })
                .orElse(BigDecimal.ZERO);
    }

    private static BigDecimal fitQtyUnderQuote(BigDecimal quoteFree, BigDecimal price, BigDecimal qty,
                                               BigDecimal step, int quoteScale) {
        if (qty == null || qty.compareTo(BigDecimal.ZERO) <= 0) return BigDecimal.ZERO;
        if (price == null || price.compareTo(BigDecimal.ZERO) <= 0) return BigDecimal.ZERO;
        BigDecimal q = qty;
        BigDecimal need = price.multiply(q).setScale(quoteScale, RoundingMode.DOWN);
        int guard = 2000;
        while (need.compareTo(quoteFree) > 0 && q.compareTo(BigDecimal.ZERO) > 0 && guard-- > 0) {
            q = q.subtract(step);
            if (q.compareTo(BigDecimal.ZERO) <= 0) return BigDecimal.ZERO;
            need = price.multiply(q).setScale(quoteScale, RoundingMode.DOWN);
        }
        return q;
    }

    private static BigDecimal parseOrZero(String s) {
        if (s == null || s.isBlank()) return BigDecimal.ZERO;
        try { return new BigDecimal(s); } catch (NumberFormatException e) { return BigDecimal.ZERO; }
    }

    private static BigDecimal bd(double d) { return new BigDecimal(Double.toString(d)); }

    private static BigDecimal floorToStep(BigDecimal value, BigDecimal step) {
        if (step == null || step.signum() == 0) return value;
        BigDecimal n = value.divide(step, 0, RoundingMode.DOWN);
        return n.multiply(step);
    }

    private static int scaleFromStepString(String step) {
        int idx = step.indexOf('.');
        return idx < 0 ? 0 : (step.length() - idx - 1);
    }

    private static String fmt(BigDecimal v, int scale) {
        if (v == null) return "0";
        return v.setScale(Math.max(scale, 0), RoundingMode.DOWN).stripTrailingZeros().toPlainString();
    }

    private BigDecimal tickFromBook(String symbol) {
        BookTicker t = mexc.bookTicker(symbol);
        String p = (t.getAskPrice() != null && !t.getAskPrice().isBlank()) ? t.getAskPrice() : t.getBidPrice();
        int scale = (p != null && p.contains(".")) ? (p.length() - p.indexOf('.') - 1) : 0;
        if (scale < 0) scale = 0;
        return BigDecimal.ONE.movePointLeft(scale);
    }

    private int qtyToScale(BigDecimal qty) {
        // на случай, если по символу qtyScale большой, а qty получилась «короткой» — лишних нулей не подставляем
        String s = qty.stripTrailingZeros().toPlainString();
        int idx = s.indexOf('.');
        return idx < 0 ? 0 : (s.length() - idx - 1);
    }
}
