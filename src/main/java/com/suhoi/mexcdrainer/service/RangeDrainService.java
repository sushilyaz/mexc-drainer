package com.suhoi.mexcdrainer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.math.BigDecimal.ZERO;

/**
 * Перелив в ЗАДАННОМ диапазоне цен [rangeLow; rangeHigh].
 * Повторяет алгоритм обычного DrainService (SEED + две "ноги"), но:
 * - строго держит операции внутри указанного диапазона (цены A SELL/BUY),
 * - контролирует, что диапазон целиком лежит внутри актуального спреда [bid; ask] и спред шире диапазона,
 * - останавливается при нарушении условий (дублируется монитором спреда).
 * <p>
 * Итерация шага:
 * 1) A: LIMIT-SELL на нижней кромке диапазона (ceil по тик-сетке)
 * 2) B: MARKET-BUY в эту заявку (агрессивно)
 * 3) A: LIMIT-BUY на верхней кромке диапазона (floor по тик-сетке, лимит с бюджетом и maxQty = qty)
 * 4) B: MARKET-SELL в эту заявку (агрессивно)
 * <p>
 * Учёт "перелитой" суммы ведём оценочно: (buyPrice - sellPrice) * qty.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RangeDrainService {

    // ===== Настройки (можно подстроить под себя) =====
    /**
     * Период мониторинга и частоты чтения стакана.
     */
    private static final Duration MONITOR_PERIOD = Duration.ofMillis(300);
    /**
     * "Тиковая" перестраховка мониторинга (0..3).
     */
    private static final int MONITOR_TICK_SAFETY = 1;
    /**
     * Максимальный бюджет USDT на один микрошаг (как и в обычном переливе, чтобы не долбить рынок).
     */
    private static final BigDecimal MAX_STEP_USDT = new BigDecimal("5");
    /**
     * Запас к плановому qty от "остатка цели" (~2%).
     */
    private static final BigDecimal PLAN_QTY_SAFETY = new BigDecimal("0.98");
    /**
     * Пауза между шагами.
     */
    private static final long SLEEP_MS = 250L;
    /**
     * Запас к бюджету SEED-докупки (учёт ask+тики и проскальзывания).
     */
    private static final BigDecimal SEED_SAFETY = new BigDecimal("1.03");

    private final MexcTradeService mexc;
    private final SpreadMonitorService monitor;

    // простой unsigned клиент для bookTicker
    private final RestTemplate rest = new RestTemplate();
    private final ObjectMapper om = new ObjectMapper();

    /**
     * БЛОКИРУЮЩИЙ запуск. В боте вызывай в отдельном потоке.
     */
    public void startDrainInRange(Long chatId,
                                  String symbol,
                                  BigDecimal rangeLow,
                                  BigDecimal rangeHigh,
                                  BigDecimal targetUsdt) {

        final String session = UUID.randomUUID().toString().substring(0, 8);

        // --- Валидация входных параметров
        if (chatId == null) throw new IllegalArgumentException("chatId is null");
        if (symbol == null || symbol.isBlank()) throw new IllegalArgumentException("symbol is blank");
        if (rangeLow == null || rangeHigh == null || targetUsdt == null)
            throw new IllegalArgumentException("rangeLow/rangeHigh/targetUsdt must be non-null");
        if (rangeLow.compareTo(ZERO) <= 0 || rangeHigh.compareTo(ZERO) <= 0 || rangeLow.compareTo(rangeHigh) >= 0)
            throw new IllegalArgumentException("Неверный диапазон цен: " + rangeLow + " .. " + rangeHigh);
        if (targetUsdt.compareTo(new BigDecimal("1")) < 0)
            throw new IllegalArgumentException("Слишком маленькая цель (минимум ~1 USDT): " + targetUsdt);

        final BigDecimal low = strip(rangeLow);
        final BigDecimal high = strip(rangeHigh);
        final BigDecimal target = strip(targetUsdt);

        log.info("🚀 [{}] RANGE start: symbol={} | range=[{}..{}] | target={} USDT",
                session, symbol, low, high, target);

        // --- Мониторинг спреда (авто-стоп при схлопывании)
        AtomicBoolean stop = new AtomicBoolean(false);
        var handle = monitor.startMonitor(
                new SpreadMonitorService.MonitorConfig(
                        symbol, low, high, MONITOR_PERIOD, MONITOR_TICK_SAFETY, true, chatId
                ),
                snap -> {
                    stop.set(true);
                    log.warn("🛑 [{}] Monitor stop: bid={} ask={} spread={}",
                            session, strip(snap.getBid()), strip(snap.getAsk()), strip(snap.getSpread()));
                }
        );

        try {
            // === SEED: докупить токенов на A при необходимости (как в обычном переливе)
            seedIfNeeded(chatId, symbol, low, high, session, stop);

            // === Главный цикл
            BigDecimal transferred = ZERO; // оценочно "перелито" USDT
            int step = 0;

            while (!stop.get() && transferred.compareTo(target) < 0) {
                step++;

                // 1) Актуальный стакан и проверка условий диапазона/спреда
                var book = fetchBook(symbol);
                if (book == null || book.bid.signum() <= 0 || book.ask.signum() <= 0) {
                    log.warn("({}) [{}] Пустой стакан, пауза {}ms", step, session, SLEEP_MS);
                    sleep(SLEEP_MS);
                    continue;
                }
                if (!rangeInsideSpread(low, high, book.bid, book.ask)) {
                    log.warn("({}) [{}] Диапазон не внутри спреда (bid={} ask={} spread={} vs band={}). Стоп.",
                            step, session,
                            strip(book.bid), strip(book.ask),
                            strip(book.ask.subtract(book.bid)), strip(high.subtract(low)));
                    break;
                }

                // 2) Цены внутри [low; high] ∩ [bid; ask], выравнивание к сетке
                BigDecimal innerLow = max(low, book.bid);
                BigDecimal innerHigh = min(high, book.ask);

                BigDecimal sellPrice = mexc.alignPriceCeil(symbol, innerLow);   // нижняя кромка для SELL A
                BigDecimal buyPrice = mexc.alignPriceFloor(symbol, innerHigh); // верхняя кромка для BUY A
                if (sellPrice.compareTo(buyPrice) >= 0) {
                    log.warn("({}) [{}] Некорректные цены после выравнивания: sell={} buy={}",
                            step, session, strip(sellPrice), strip(buyPrice));
                    sleep(200);
                    continue;
                }

                BigDecimal delta = buyPrice.subtract(sellPrice);
                if (delta.signum() <= 0) {
                    log.warn("({}) [{}] delta<=0 (sell={} buy={})", step, session, strip(sellPrice), strip(buyPrice));
                    sleep(200);
                    continue;
                }

                // 3) Плановый объём шага (как в обычном переливе: остаток/дельта и лимит по бюджету шага)
                BigDecimal remaining = target.subtract(transferred);
                BigDecimal qtyByRemaining = safeDiv(remaining, delta).multiply(PLAN_QTY_SAFETY);
                BigDecimal qtyByStepBudget = safeDiv(MAX_STEP_USDT.min(remaining), sellPrice);

                BigDecimal planQty = mexc.alignQtyFloor(symbol, minPos(qtyByRemaining, qtyByStepBudget));
                if (planQty.compareTo(ZERO) <= 0) {
                    log.warn("({}) [{}] planQty<=0 (remaining={} delta={} sellPrice={}) — стоп.",
                            step, session, strip(remaining), strip(delta), strip(sellPrice));
                    break;
                }

                // 4) Guard от Oversold: перечитали баланс A и обрезали qty до доступного
                BigDecimal balA = mexc.getTokenBalanceAccountA(symbol, chatId);
                BigDecimal sellQty = mexc.alignQtyFloor(symbol, min(planQty, balA));
                if (sellQty.compareTo(planQty) < 0) {
                    log.warn("({}) [{}] Oversold-guard: план={} > балансA={} ⇒ продаю {}",
                            step, session, strip(planQty), strip(balA), strip(sellQty));
                }
                if (sellQty.signum() <= 0) {
                    // попробуем докупить немного токена (микро-SEED) и повторить на этой итерации
                    BigDecimal deficit = planQty.subtract(balA).max(new BigDecimal("0.0"));
                    if (deficit.signum() > 0) {
                        BigDecimal estBudget = buyPrice.multiply(deficit).multiply(SEED_SAFETY).max(new BigDecimal("1"));
                        log.info("({}) [{}] Микро-SEED: deficitQty={} budget~{} (buyPrice={})",
                                step, session, strip(deficit), strip(estBudget), strip(buyPrice));
                        var seedOrder = mexc.limitBuyAboveSpreadAccountA(symbol, estBudget, chatId);
                        log.info("({}) [{}] Микро-SEED result: status={} executedQty={} avgPrice={}",
                                step, session, seedOrder.status(), strip(seedOrder.executedQty()), strip(seedOrder.avgPrice()));
                    }
                    sleep(200);
                    continue;
                }

                log.info("({}) [{}] План шага: sell={} buy={} delta={} qty={} remaining={}",
                        step, session, strip(sellPrice), strip(buyPrice), strip(delta), strip(sellQty), strip(remaining));

                // === НОГА 1: A SELL у нижней кромки диапазона
                String sellId = mexc.placeLimitSellAccountA(symbol, sellPrice, sellQty, chatId);
                if (sellId == null) {
                    log.warn("({}) [{}] SELL A не размещён (вероятно minQty/minNotional). Пропуск.", step, session);
                    sleep(300);
                    continue;
                }
                log.info("({}) [{}] A ➡ SELL лимитка qty={} @ {} (orderId={})",
                        step, session, strip(sellQty), strip(sellPrice), sellId);

                // === НОГА 1b: B MARKET-BUY на sellQty
                mexc.marketBuyFromAccountB(symbol, sellPrice, sellQty, chatId);
                log.info("({}) [{}] B ➡ BUY market ~{} @ {}", step, session, strip(sellQty), strip(sellPrice));

                // === НОГА 2: A BUY у верхней кромки (бюджет ограничиваем и фиксируем maxQty = sellQty)
                BigDecimal budgetABuy = mexc.reserveForMakerFee(buyPrice.multiply(sellQty));
                String buyId = mexc.placeLimitBuyAccountA(symbol, buyPrice, budgetABuy, sellQty, chatId);
                if (buyId == null) {
                    log.warn("({}) [{}] BUY A не размещён — шаг без второй ноги.", step, session);
                } else {
                    log.info("({}) [{}] A ➡ BUY лимитка maxQty={} @ {} (orderId={})",
                            step, session, strip(sellQty), strip(buyPrice), buyId);
                }

                // === НОГА 2b: B MARKET-SELL в эту лимитку
                mexc.marketSellFromAccountB(symbol, buyPrice, sellQty, chatId);
                log.info("({}) [{}] B ➡ SELL market ~{} @ {}", step, session, strip(sellQty), strip(buyPrice));

                // --- учёт «перелитой» суммы за шаг
                BigDecimal stepTransferred = delta.multiply(sellQty);
                transferred = transferred.add(stepTransferred);
                log.info("({}) [{}] Шаг завершён: ~перелито {} USDT (итого {} / {})",
                        step, session, strip(stepTransferred), strip(transferred.min(target)), strip(target));

                sleep(SLEEP_MS);
            }

            if (stop.get()) {
                log.warn("🟡 [{}] RANGE stopped by spread monitor. Итог ~{} / {} USDT",
                        session, strip(transferredOrZero(transferred)), strip(target));
            } else if (transferredOrZero(transferred).compareTo(target) >= 0) {
                log.info("✅ [{}] RANGE done: достигли цели {} USDT", session, strip(target));
            } else {
                log.warn("🟡 [{}] RANGE finished early. Итог ~{} / {} USDT",
                        session, strip(transferredOrZero(transferred)), strip(target));
            }

        } finally {
            try {
                handle.close();
            } catch (Exception ignore) {
            }
        }
    }

    // ===== SEED-блок (как в обычном переливе): докупить минимум токена на A, чтобы начать цикл =====
    private void seedIfNeeded(Long chatId,
                              String symbol,
                              BigDecimal low,
                              BigDecimal high,
                              String session,
                              AtomicBoolean stop) {
        while (!stop.get()) {
            var book = fetchBook(symbol);
            if (book == null || book.bid.signum() <= 0 || book.ask.signum() <= 0) {
                log.warn("({}) [{}] SEED: пустой стакан, пауза {}ms", 0, session, SLEEP_MS);
                sleep(SLEEP_MS);
                continue;
            }
            if (!rangeInsideSpread(low, high, book.bid, book.ask)) {
                log.warn("({}) [{}] SEED: диапазон не внутри спреда (bid={} ask={}). Старт невозможен.",
                        0, session, strip(book.bid), strip(book.ask));
                throw new IllegalStateException("Диапазон вне спреда на старте.");
            }

            BigDecimal innerLow = max(low, book.bid);
            BigDecimal innerHigh = min(high, book.ask);
            BigDecimal sellPrice = mexc.alignPriceCeil(symbol, innerLow);
            BigDecimal buyPrice = mexc.alignPriceFloor(symbol, innerHigh);
            if (sellPrice.compareTo(buyPrice) >= 0) {
                sleep(200);
                continue;
            }

            // Возьмём небольшой базовый seed-объём: половина MAX_STEP_USDT по нижней цене
            BigDecimal seedBudget = MAX_STEP_USDT.divide(new BigDecimal("2"), 18, RoundingMode.DOWN);
            BigDecimal seedQtyPlan = mexc.alignQtyFloor(symbol, safeDiv(seedBudget, sellPrice));
            if (seedQtyPlan.compareTo(ZERO) <= 0) {
                seedBudget = new BigDecimal("1");
                seedQtyPlan = mexc.alignQtyFloor(symbol, safeDiv(seedBudget, sellPrice));
                if (seedQtyPlan.compareTo(ZERO) <= 0) break; // нечего сеять — пусть цикл сам доберёт
            }

            BigDecimal balA = mexc.getTokenBalanceAccountA(symbol, chatId);
            if (balA.compareTo(seedQtyPlan) >= 0) {
                log.info("({}) [{}] SEED: достаточно токенов на A: have={} need={}",
                        0, session, strip(balA), strip(seedQtyPlan));
                break;
            }

            BigDecimal deficitQty = seedQtyPlan.subtract(balA);
            BigDecimal estBudget = buyPrice.multiply(deficitQty).multiply(SEED_SAFETY).max(new BigDecimal("1"));

            log.info("({}) [{}] SEED: докупаю на A deficitQty={} бюджет~{} (buyPrice={})",
                    0, session, strip(deficitQty), strip(estBudget), strip(buyPrice));

            var seedOrder = mexc.limitBuyAboveSpreadAccountA(symbol, estBudget, chatId);
            log.info("({}) [{}] SEED result: status={} executedQty={} avgPrice={}",
                    0, session, seedOrder.status(), strip(seedOrder.executedQty()), strip(seedOrder.avgPrice()));
            break; // даже если купили меньше — основной цикл досеет микро-SEEDом
        }
    }

    // ===== helpers =====

    private record Book(BigDecimal bid, BigDecimal ask) {
    }

    private Book fetchBook(String symbol) {
        try {
            String url = "https://api.mexc.com/api/v3/ticker/bookTicker?symbol=" + symbol;
            String body = rest.getForObject(url, String.class);
            JsonNode j = om.readTree(Objects.requireNonNull(body));

            BigDecimal bid = new BigDecimal(j.path("bidPrice").asText("0"));
            BigDecimal ask = new BigDecimal(j.path("askPrice").asText("0"));
            if (bid.signum() <= 0 && ask.signum() > 0) bid = ask;
            if (ask.signum() <= 0 && bid.signum() > 0) ask = bid;
            if (bid.signum() <= 0 && ask.signum() <= 0) return null;
            return new Book(bid, ask);
        } catch (Exception e) {
            log.warn("Ошибка чтения bookTicker {}: {}", symbol, e.getMessage());
            return null;
        }
    }

    private static boolean rangeInsideSpread(BigDecimal low, BigDecimal high, BigDecimal bid, BigDecimal ask) {
        if (bid == null || ask == null) return false;
        BigDecimal spread = ask.subtract(bid);
        BigDecimal band = high.subtract(low);
        return spread.signum() > 0
                && spread.compareTo(band) >= 0
                && bid.compareTo(low) <= 0
                && ask.compareTo(high) >= 0;
    }

    private static BigDecimal safeDiv(BigDecimal num, BigDecimal den) {
        if (num == null || den == null || den.compareTo(ZERO) == 0) return ZERO;
        return num.divide(den, 18, RoundingMode.DOWN);
    }

    private static BigDecimal minPos(BigDecimal a, BigDecimal b) {
        if (a == null || a.compareTo(ZERO) <= 0) return (b == null ? ZERO : b.max(ZERO));
        if (b == null || b.compareTo(ZERO) <= 0) return a.max(ZERO);
        return (a.compareTo(b) <= 0) ? a : b;
    }

    private static BigDecimal min(BigDecimal a, BigDecimal b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static BigDecimal max(BigDecimal a, BigDecimal b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    private static BigDecimal strip(BigDecimal x) {
        return x.stripTrailingZeros();
    }

    private static BigDecimal transferredOrZero(BigDecimal x) {
        return x == null ? ZERO : x;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}
