package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.model.RangeState;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.util.HexFormat;

import static java.math.BigDecimal.ZERO;

/**
 * Перелив USDT в ЗАДАННОМ ценовом диапазоне БЕЗ контроля спреда.
 *
 * Алгоритм шага (повторяет логику обычного DrainService, но с фиксированными ценами из диапазона):
 *   1) A: LIMIT SELL @ low, qty = min(plan, balanceA)  (если токена не хватает — "seed")
 *   2) B: MARKET BUY на ту же qty (чтобы удовлетворить нашу лимитку A)
 *   3) A: LIMIT BUY  @ high на бюджет ≈ qty * high (maxQty = qty, чтобы не перекупить)
 *   4) B: MARKET SELL той же qty (закрываем лимитку A BUY)
 *
 * Цель: перелить примерно targetUsdt (накопительно delta * qty за все шаги),
 * где delta = high - low.
 *
 * ВНИМАНИЕ: Контроля спреда НЕТ — бот просто работает внутри указанного пользователем диапазона.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RangeDrainService {

    private final MexcTradeService mexc;

    /** Пауза между шагами, мс (чтобы не душить API/матчинг). */
    private static final long STEP_SLEEP_MS = 350L;

    /** Максимум попыток "подсева" при нехватке токена на A. */
    private static final int SEED_MAX_RETRIES = 3;

    /** Накидка 5% на бюджет под "seed"-покупку (безопасный запас на проскальзывание/комиссии). */
    private static final BigDecimal SEED_BUDGET_K = new BigDecimal("1.05");

    /** Служебный id запуска — для удобства в логах. */
    private static String runId() {
        byte[] b = new byte[4];
        new SecureRandom().nextBytes(b);
        return HexFormat.of().formatHex(b);
    }

    // =========================================================================================
    // ПЕРЕГРУЗКИ ПОД ТВОЙ ВЫЗОВ И УДОБСТВО ИСПОЛЬЗОВАНИЯ
    // =========================================================================================

    /**
     * ТВОЙ ФОРМАТ ВЫЗОВА (из TelegramBotHandler):
     *   rangeDrainService.startDrainInRange(chatId, symbol, low, high, usdt);
     */
    public void startDrainInRange(Long chatId,
                                  String symbol,
                                  BigDecimal rangeLow,
                                  BigDecimal rangeHigh,
                                  BigDecimal targetUsdt) {
        startDrainInRange(symbol, rangeLow, rangeHigh, targetUsdt, chatId, 50);
    }

    /**
     * Та же перегрузка, но с кастомным лимитом шагов.
     */
    public void startDrainInRange(Long chatId,
                                  String symbol,
                                  BigDecimal rangeLow,
                                  BigDecimal rangeHigh,
                                  BigDecimal targetUsdt,
                                  int maxSteps) {
        startDrainInRange(symbol, rangeLow, rangeHigh, targetUsdt, chatId, maxSteps);
    }

    // =========================================================================================
    // ОСНОВНОЙ МЕТОД (ядро)
    // =========================================================================================

    /**
     * Основной запуск перелива внутри диапазона (без мониторинга спреда).
     */
    public void startDrainInRange(String symbol,
                                  BigDecimal rangeLow,
                                  BigDecimal rangeHigh,
                                  BigDecimal targetUsdt,
                                  Long chatId,
                                  int maxSteps) {

        // ---------- Валидация ----------
        if (symbol == null || rangeLow == null || rangeHigh == null || targetUsdt == null) {
            throw new IllegalArgumentException("Пустые параметры запуска диапазонного перелива");
        }
        MemoryDb.saveNewRangeState(chatId,
               RangeState.builder()
                        .symbol(symbol)
                        .rangeLow(rangeLow)
                        .rangeHigh(rangeHigh)
                        .targetUsdt(targetUsdt)
                        .drainedUsdt(BigDecimal.ZERO)
                        .step(0)
                        .paused(false)
                        .running(true)
                        .build()
        );
        symbol = symbol.trim().toUpperCase();

        rangeLow  = rangeLow.stripTrailingZeros();
        rangeHigh = rangeHigh.stripTrailingZeros();

        if (rangeLow.compareTo(ZERO) <= 0 || rangeHigh.compareTo(rangeLow) <= 0) {
            throw new IllegalArgumentException("Невалидный диапазон цен: " + rangeLow + " .. " + rangeHigh);
        }
        if (targetUsdt.compareTo(new BigDecimal("0.50")) < 0) {
            log.warn("⚠️ targetUsdt={} слишком мало — возможны холостые действия.",
                    targetUsdt.stripTrailingZeros().toPlainString());
        }

        final String rid = runId();
        final BigDecimal sellPrice = rangeLow;   // низ диапазона
        final BigDecimal buyPrice  = rangeHigh;  // верх диапазона
        final BigDecimal delta     = buyPrice.subtract(sellPrice);

        if (delta.compareTo(ZERO) <= 0) {
            throw new IllegalArgumentException("Верх диапазона должен быть выше низа (delta <= 0).");
        }

        log.info("🚀 [{}] RANGE start: symbol={} | range=[{}..{}] | target={} USDT",
                rid, symbol,
                sellPrice.toPlainString(),
                buyPrice.toPlainString(),
                targetUsdt.stripTrailingZeros().toPlainString());

        BigDecimal drainedUsdt = ZERO; // накопительно «перелито»
        int step = 0;

        // ---------- Основной цикл ----------
        while (drainedUsdt.compareTo(targetUsdt) < 0 && step < Math.max(1, maxSteps)) {
            var state = MemoryDb.getRangeState(chatId);
            if (state != null && state.isPaused()) {
                log.warn("⏸️ [{}] RANGE paused по команде /stop. Шаг={} drained={} / target={}",
                        rid, step,
                        drainedUsdt.stripTrailingZeros().toPlainString(),
                        targetUsdt.stripTrailingZeros().toPlainString());
                // Просто выходим из метода. НИЧЕГО НЕ ОТМЕНЯЕМ.
                return;
            }
            step++;

            // Сколько хотим «продать на A» на этом шаге, чтобы приблизиться к цели?
            // План ≈ floor( (target - drained) / sellPrice )
            BigDecimal remaining = targetUsdt.subtract(drainedUsdt);
            BigDecimal planQty = safeFloor(remaining.divide(sellPrice, 18, RoundingMode.DOWN));
            if (planQty.compareTo(ZERO) <= 0) {
                planQty = BigDecimal.ONE; // минимально 1 «шаговая» единица (часто stepSize=1)
            }

            // Текущий баланс токена на A
            BigDecimal balanceA = mexc.getTokenBalanceAccountA(symbol, chatId).stripTrailingZeros();

            // Подсев при нехватке токена на A
            if (balanceA.compareTo(planQty) < 0) {
                BigDecimal deficit = planQty.subtract(balanceA);
                if (deficit.compareTo(BigDecimal.ONE) < 0) deficit = BigDecimal.ONE;

                BigDecimal seedBudget = buyPrice.multiply(deficit).multiply(SEED_BUDGET_K);
                log.info("({}) [{}] SEED: докупаю на A deficitQty={} по бюджету~{} (buyPrice={})",
                        (step - 1), rid,
                        deficit.stripTrailingZeros().toPlainString(),
                        seedBudget.stripTrailingZeros().toPlainString(),
                        buyPrice.toPlainString());

                boolean seeded = false;
                for (int tr = 1; tr <= SEED_MAX_RETRIES; tr++) {
                    var info = mexc.limitBuyAboveSpreadAccountA(symbol, seedBudget, chatId);
                    log.info("({}) [{}] SEED result: status={} executedQty={} avgPrice={}",
                            (step - 1), rid,
                            info.status(),
                            info.executedQty().stripTrailingZeros().toPlainString(),
                            info.avgPrice().stripTrailingZeros().toPlainString());
                    if (info.executedQty().compareTo(ZERO) > 0) {
                        seeded = true;
                        break;
                    }
                    sleepQuiet(250L);
                }
                if (!seeded) {
                    log.warn("({}) [{}] SEED: не удалось докупить токен (нет исполнения). Прерываю.",
                            (step - 1), rid);
                    break;
                }
                // обновим баланс
                balanceA = mexc.getTokenBalanceAccountA(symbol, chatId).stripTrailingZeros();
            }

            // Защита от oversold — не продаём больше, чем реально есть на A
            BigDecimal qty = planQty.min(balanceA);
            if (qty.compareTo(planQty) < 0) {
                log.warn("({}) [{}] Oversold-guard: план={} > балансA={} ⇒ продаю {}",
                        step, rid,
                        planQty.stripTrailingZeros().toPlainString(),
                        balanceA.stripTrailingZeros().toPlainString(),
                        qty.stripTrailingZeros().toPlainString());
            }

            // План шага — просто, ясно в логах
            log.info("({}) [{}] План шага: sell={} buy={} delta={} qty={} remaining={}",
                    step, rid,
                    sellPrice.toPlainString(),
                    buyPrice.toPlainString(),
                    delta.stripTrailingZeros().toPlainString(),
                    qty.stripTrailingZeros().toPlainString(),
                    remaining.stripTrailingZeros().toPlainString());

            // 1) A: LIMIT SELL @ low
            String sellOrderId = mexc.placeLimitSellAccountA(symbol, sellPrice, qty, chatId);
            if (sellOrderId == null) {
                log.warn("({}) [{}] A SELL не размещён (minNotional/minQty/валидация). Прерываю.", step, rid);
                break;
            }
            log.info("({}) [{}] A ➡ SELL лимитка qty={} @ {} (orderId={})",
                    step, rid,
                    qty.stripTrailingZeros().toPlainString(),
                    sellPrice.toPlainString(),
                    sellOrderId);

            // 2) B: MARKET BUY ~ qty @ low
            mexc.marketBuyFromAccountB(symbol, sellPrice, qty, chatId);
            log.info("({}) [{}] B ➡ BUY market ~{} @ {}",
                    step, rid,
                    qty.stripTrailingZeros().toPlainString(),
                    sellPrice.toPlainString());

            // 3) A: LIMIT BUY @ high (бюджет ≈ qty * high, maxQty = qty)
            BigDecimal budgetA = qty.multiply(buyPrice);
            BigDecimal makerBudget = mexc.reserveForMakerFee(budgetA);
            String buyOrderId = mexc.placeLimitBuyAccountA(symbol, buyPrice, makerBudget, qty, chatId);
            if (buyOrderId == null) {
                log.warn("({}) [{}] A BUY не размещён (minNotional/minQty/валидация). Прерываю.", step, rid);
                break;
            }
            log.info("({}) [{}] A ➡ BUY лимитка maxQty={} @ {} (orderId={})",
                    step, rid,
                    qty.stripTrailingZeros().toPlainString(),
                    buyPrice.toPlainString(),
                    buyOrderId);

            // 4) B: MARKET SELL ~ qty @ high
            mexc.marketSellFromAccountB(symbol, buyPrice, qty, chatId);
            log.info("({}) [{}] B ➡ SELL market ~{} @ {}",
                    step, rid,
                    qty.stripTrailingZeros().toPlainString(),
                    buyPrice.toPlainString());

            // Аппроксимация «перелитого» за шаг
            BigDecimal stepDrained = delta.multiply(qty);
            drainedUsdt = drainedUsdt.add(stepDrained);
            BigDecimal finalDrainedUsdt = drainedUsdt;
            int finalStep = step;
            MemoryDb.updateProgress(chatId, st -> {
                if (st == null) return null;
                st.setDrainedUsdt(finalDrainedUsdt);
                st.setStep(finalStep);
                st.setRangeLow(sellPrice);  // не обязательно, но пусть отражает «текущую» вилку
                st.setRangeHigh(buyPrice);
                st.setRunning(true);
                return st;
            });
            log.info("({}) [{}] Шаг завершён: ~перелито {} USDT (итого {} / {})",
                    step, rid,
                    stepDrained.stripTrailingZeros().toPlainString(),
                    drainedUsdt.stripTrailingZeros().toPlainString(),
                    targetUsdt.stripTrailingZeros().toPlainString());

            sleepQuiet(STEP_SLEEP_MS);
        }

        if (drainedUsdt.compareTo(targetUsdt) >= 0) {
            log.info("✅ [{}] RANGE done. Итог ~{} / {} USDT",
                    rid,
                    drainedUsdt.stripTrailingZeros().toPlainString(),
                    targetUsdt.stripTrailingZeros().toPlainString());
        } else {
            log.warn("🟡 [{}] RANGE stopped. Итог ~{} / {} USDT (steps={}/{})",
                    rid,
                    drainedUsdt.stripTrailingZeros().toPlainString(),
                    targetUsdt.stripTrailingZeros().toPlainString(),
                    step, maxSteps);
        }
        MemoryDb.updateProgress(chatId, st -> {
            if (st == null) return null;
            st.setRunning(false);
            return st;
        });
    }
    /**
     * Слепая пауза: просто пометить флаг paused=true.
     * Внутренний цикл остановится на следующей проверке.
     */
    public void stopRange(Long chatId) {
        com.suhoi.mexcdrainer.util.MemoryDb.markPaused(chatId);
        log.warn("⏸️ [chat={}] Диапазонный перелив переведён в паузу по /stop", chatId);
    }
    /**
     * Слепое продолжение: берём сохранённое состояние, считаем «остаток» цели и
     * просто запускаем ещё один цикл с НОВЫМ диапазоном, на той же монете.
     * НИКАКИХ сверок. Если были висящие лимитки — живём с последствиями.
     */
    public void continueRange(Long chatId, BigDecimal newLow, BigDecimal newHigh) {
        var st = com.suhoi.mexcdrainer.util.MemoryDb.getRangeState(chatId);
        if (st == null || st.getSymbol() == null) {
            throw new IllegalStateException("Нет активной диапазонной сессии для этого чата");
        }
        BigDecimal drained = st.getDrainedUsdt() == null ? BigDecimal.ZERO : st.getDrainedUsdt();
        BigDecimal target = st.getTargetUsdt() == null ? BigDecimal.ZERO : st.getTargetUsdt();

        BigDecimal remaining = target.subtract(drained);
        if (remaining.signum() <= 0) {
            log.info("▶️ [chat={}] Нечего продолжать: уже достигнута цель {} USDT (drained={})",
                    chatId, target.stripTrailingZeros(), drained.stripTrailingZeros());
            return;
        }

        // Сбрасываем флаг паузы и обновляем вилку в снапшоте
        com.suhoi.mexcdrainer.util.MemoryDb.updateProgress(chatId, s -> {
            if (s == null) return null;
            s.setPaused(false);
            s.setRunning(true);
            s.setRangeLow(newLow);
            s.setRangeHigh(newHigh);
            return s;
        });

        log.info("▶️ [chat={}] CONTINUE {} с новой вилкой [{} .. {}], остаток цели ~{} USDT",
                chatId, st.getSymbol(), newLow.toPlainString(), newHigh.toPlainString(),
                remaining.stripTrailingZeros().toPlainString());

        // Запускаем ещё один цикл на остаток
        startDrainInRange(chatId, st.getSymbol(), newLow, newHigh, remaining);
    }

    // =========================================================================================
    // ВСПОМОГАТЕЛЬНОЕ
    // =========================================================================================

    /** Для планирования qty — оставляем целую часть (часто stepSize=1). */
    private static BigDecimal safeFloor(BigDecimal v) {
        if (v == null) return ZERO;
        if (v.signum() <= 0) return ZERO;
        return v.setScale(0, RoundingMode.DOWN).stripTrailingZeros();
    }

    private static void sleepQuiet(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { }
    }
}
