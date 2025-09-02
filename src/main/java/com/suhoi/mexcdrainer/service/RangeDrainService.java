package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.model.RangeState;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

import static java.math.BigDecimal.ZERO;

/**
 * Перелив USDT в указанном диапазоне.
 * Ключевые моменты:
 *  - Планирование объёма на шаг идёт по бюджету шага (в USDT на нижней границе),
 *    а не по попытке "закрыть цель за 1 шаг".
 *  - Для tiny-ценовых монет гарантируем минимальную qty (stepSize/minNotional) ≠ 0.
 *  - Автопауза не отменяет ордера из другого потока (без гонок).
 *  - Ручные /stop, /continue, /status.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RangeDrainService {

    private final MexcTradeService mexc;
    private final SpreadMonitorService spread;

    /** Пауза между шагами, мс (щадим API/матчинг). */
    private static final long STEP_SLEEP_MS = 350L;

    /** Накидка к бюджету SEED-покупки (5%). */
    private static final BigDecimal SEED_BUDGET_K = new BigDecimal("1.05");

    /** Верхний лимит бюджета одного шага в USDT на нижней границе (можно вынести в конфиг). */
    private static final BigDecimal MAX_STEP_BUDGET_USDT = new BigDecimal("3"); // под твои тесты

    private static final boolean AUTO_PAUSE_FROM_SPREAD = false;

    // =========================================================================================
    // Публичные методы под Telegram
    // =========================================================================================

    public void startDrainInRange(Long chatId,
                                  String symbol,
                                  BigDecimal rangeLow,
                                  BigDecimal rangeHigh,
                                  BigDecimal targetUsdt) {
        startDrainInRange(symbol, rangeLow, rangeHigh, targetUsdt, chatId, 50);
    }

    public void startDrainInRange(String symbol,
                                  BigDecimal rangeLow,
                                  BigDecimal rangeHigh,
                                  BigDecimal targetUsdt,
                                  Long chatId,
                                  int maxSteps) {

        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(rangeLow, "rangeLow");
        Objects.requireNonNull(rangeHigh, "rangeHigh");
        Objects.requireNonNull(targetUsdt, "targetUsdt");

        symbol = symbol.trim().toUpperCase();
        if (rangeHigh.subtract(rangeLow).signum() <= 0) {
            throw new IllegalArgumentException("HIGH должно быть строго выше LOW");
        }

        RangeState init = RangeState.builder()
                .symbol(symbol)
                .rangeLow(rangeLow.stripTrailingZeros())
                .rangeHigh(rangeHigh.stripTrailingZeros())
                .targetUsdt(targetUsdt)
                .drainedUsdt(ZERO)
                .step(0)
                .paused(false)
                .running(true)
                .updatedAt(Instant.now())
                .build();
        MemoryDb.saveNewRangeState(chatId, init);

        final String rid = runId();
        final BigDecimal sellPrice = init.getRangeLow();
        final BigDecimal buyPrice  = init.getRangeHigh();
        final BigDecimal delta     = buyPrice.subtract(sellPrice); // разница границ диапазона

        // Монитор спреда: только помечаем paused (без отмены из этого же потока!)
        SpreadMonitorService.MonitorConfig monCfg = new SpreadMonitorService.MonitorConfig(
                symbol, sellPrice, buyPrice,
                java.time.Duration.ofMillis(150),
                1,          // tickSafety
                true,       // excludeSelf
                chatId
        );
        final String symFinal = symbol;
        if (AUTO_PAUSE_FROM_SPREAD) {
            spread.startMonitor(monCfg, snap -> {
                setStatus(chatId, "⛔ Вилка вышла из спреда: bid=" + snap.getBid() + ", ask=" + snap.getAsk());
                requestPause(chatId, symFinal, "Диапазон вышел из спреда (bid=" + snap.getBid() + ", ask=" + snap.getAsk() + ")");
            });
        } else {
            // монитор оставляем для телеметрии, но без автопаузы
            spread.startMonitor(monCfg, snap -> {
                if (log.isDebugEnabled()) {
                    log.debug("SPREAD[{}|{}|NO-AUTOPAUSE] bid={} ask={} spread={} | range=[{}..{}]",
                            symFinal, chatId,
                            snap.getBid(), snap.getAsk(),
                            snap.getAsk().subtract(snap.getBid()),
                            sellPrice, buyPrice);
                }
                // статус писать не обязательно — чтобы не спамить
                // setStatus(chatId, "👀 Монитор без автопаузы: bid=" + snap.getBid() + ", ask=" + snap.getAsk());
            });
        }


        log.info("🚀 [{}] RANGE start: {} [{}/{}], target={} USDT",
                rid, symbol,
                sellPrice.toPlainString(), buyPrice.toPlainString(),
                targetUsdt.stripTrailingZeros().toPlainString());
        setStatus(chatId, "🚀 Старт RANGE: " + symbol + " [" + sellPrice + ".." + buyPrice + "], цель=" + targetUsdt.stripTrailingZeros());

        BigDecimal drainedUsdt = nz(init.getDrainedUsdt());
        int step = nzInt(init.getStep());

        // Основной цикл
        while (drainedUsdt.compareTo(targetUsdt) < 0 && step < Math.max(1, maxSteps)) {
            RangeState st = MemoryDb.getRangeState(chatId);
            if (st == null || st.isPaused()) {
                log.warn("⏸️ [{}] Остановлено (paused). Выход из цикла.", rid);
                break;
            }
            step++;

            // Сколько ещё нужно "перелить" и какой бюджет на шаг используем
            BigDecimal remaining = targetUsdt.subtract(drainedUsdt);
            BigDecimal stepBudget = remaining.min(MAX_STEP_BUDGET_USDT); // ключевой момент

            // Планируем qty по бюджету на нижней границе
            BigDecimal planQty = floorPositive(divSafe(stepBudget, sellPrice));

            // Гарантируем минимум по шагу/ноционалу
            BigDecimal minQtyByStep = mexc.normalizeQtyForSymbol(symbol, BigDecimal.ONE); // обычно 1, если stepSize=1
            if (minQtyByStep.signum() <= 0) minQtyByStep = BigDecimal.ONE;               // страховка
            BigDecimal minQtyByNotional = mexc.planSellMinQtyForNotional(symbol, sellPrice);
            BigDecimal minQtyRequired = max(minQtyByStep, minQtyByNotional);

            if (planQty.compareTo(minQtyRequired) < 0) {
                planQty = minQtyRequired;
            }

            // Учитываем фактический баланс A; при нехватке — SEED до нужного уровня
            BigDecimal balanceA = mexc.getTokenBalanceAccountA(symbol, chatId).stripTrailingZeros();
            if (balanceA.compareTo(planQty) < 0) {
                boolean seeded = ensureSeed(symbol, buyPrice, planQty, chatId);
                if (!seeded) {
                    requestPause(chatId, symbol, "Недостаточно токена на A, SEED не удался.");
                    break;
                }
                balanceA = mexc.getTokenBalanceAccountA(symbol, chatId).stripTrailingZeros();
                if (balanceA.compareTo(planQty) < 0) {
                    // На всякий случай режем до того, что есть (после SEED должно хватать, но пусть будет)
                    planQty = balanceA;
                }
            }

            // Финальная нормализация
            BigDecimal qty = mexc.normalizeQtyForSymbol(symbol, planQty);
            if (qty.signum() <= 0) {
                requestPause(chatId, symbol, "После нормализации qty=0 — шаг невозможен (stepSize/minQty).");
                break;
            }

            // === 1) A: LIMIT SELL @ low ===
            String sellOrderId = mexc.placeLimitSellAccountA(symbol, sellPrice, qty, chatId);
            if (sellOrderId == null) {
                requestPause(chatId, symbol, "SELL A не размещён (minNotional/minQty).");
                break;
            }
            if (pausedNow(chatId)) {
                log.warn("({}) [{}] Пауза после A SELL — отменяю и выхожу", step, rid);
                cancelBothSafely(symbol, chatId);
                return;
            }
            log.info("({}) [{}] A SELL placed: id={}, qty={}, price={}",
                    step, rid, sellOrderId, qty.stripTrailingZeros(), sellPrice);
            setStatus(chatId, "A SELL: id=" + sellOrderId + ", qty=" + qty.stripTrailingZeros() + ", price=" + sellPrice);

// === 2) B: MARKET BUY этой же qty (агрессивно) ===
            mexc.marketBuyFromAccountB(symbol, sellPrice, qty, chatId);
            setStatus(chatId, "B BUY: qty=" + qty.stripTrailingZeros() + " @~" + sellPrice);

// Подтверждаем, что A-SELL закрыт, и берём реальный бюджет на A-BUY
            var credsA = com.suhoi.mexcdrainer.util.MemoryDb.getAccountA(chatId);
            MexcTradeService.OrderInfo sellAInfo = null;
            if (credsA != null) {
                sellAInfo = mexc.waitUntilFilled(symbol, sellOrderId, credsA.getApiKey(), credsA.getSecret(), 6000);
            }
            if (sellAInfo == null || sellAInfo.executedQty().signum() <= 0) {
                requestPause(chatId, symbol, "A SELL не FILLED или executedQty=0 после B BUY.");
                break;
            }
            BigDecimal usdtEarned = sellAInfo.cummQuoteQty();

// === 3) A: LIMIT BUY @ high — бюджет строго от usdtEarned ===
            BigDecimal budget = mexc.reserveForMakerFee(usdtEarned);

// вычисляем сколько максимум монет реально можно купить на этот бюджет по high
            BigDecimal buyCapByBudget = budget.divide(buyPrice, 18, RoundingMode.DOWN);
            BigDecimal buyCapNorm = mexc.normalizeQtyForSymbol(symbol, buyCapByBudget);
            BigDecimal placedBuyQty = qty.min(buyCapNorm);

// если после нормализации купить нечего — пауза
            if (placedBuyQty.signum() <= 0) {
                requestPause(chatId, symbol, "A BUY: бюджет от SELL не позволяет купить ≥ minQty/minNotional.");
                break;
            }

            String buyOrderId = mexc.placeLimitBuyAccountA(symbol, buyPrice, budget, placedBuyQty, chatId);
            if (buyOrderId == null) {
                requestPause(chatId, symbol, "BUY A не размещён (budget/minNotional).");
                break;
            }
            log.info("({}) [{}] A BUY placed: id={}, maxQty={}, price={}",
                    step, rid, buyOrderId, placedBuyQty.stripTrailingZeros(), buyPrice);
            setStatus(chatId, "A BUY: id=" + buyOrderId + ", maxQty=" + placedBuyQty.stripTrailingZeros() + ", price=" + buyPrice);

// важно: пауза могла случиться СЕЙЧАС — проверяем ещё раз перед B SELL
            if (pausedNow(chatId)) {
                log.warn("({}) [{}] Пауза после A BUY — отменяю и выхожу", step, rid);
                cancelBothSafely(symbol, chatId);
                return;
            }

// === 4) B: MARKET SELL ровно в объём placedBuyQty ===
            mexc.marketSellFromAccountB(symbol, buyPrice, placedBuyQty, chatId);
            setStatus(chatId, "B SELL: qty=" + placedBuyQty.stripTrailingZeros() + " @~" + buyPrice);

// (опционально) Быстрая верификация, что A BUY закрылся
            MexcTradeService.OrderInfo buyAInfo = null;
            if (credsA != null) {
                buyAInfo = mexc.waitUntilFilled(symbol, buyOrderId, credsA.getApiKey(), credsA.getSecret(), 6000);
            }
            if (buyAInfo == null || buyAInfo.executedQty().signum() <= 0) {
                requestPause(chatId, symbol, "A BUY не FILLED после B SELL (вероятен перехват/накрут).");
                break;
            }

// Итог шага: считаем перелив по фактическому объёму, который мы заложили в BUY
            BigDecimal stepDrained = delta.multiply(placedBuyQty);
            drainedUsdt = drainedUsdt.add(stepDrained);

            int finalStep = step;
            BigDecimal finalDrainedUsdt = drainedUsdt;
            MemoryDb.updateProgress(chatId, s -> {
                if (s == null) return null;
                s.setDrainedUsdt(finalDrainedUsdt);
                s.setStep(finalStep);
                s.setUpdatedAt(Instant.now());
                s.setRunning(true);
                return s;
            });

            log.info("({}) [{}] Шаг завершён: ~перелито {} USDT (итого {} / {})",
                    step, rid,
                    stepDrained.stripTrailingZeros().toPlainString(),
                    drainedUsdt.stripTrailingZeros().toPlainString(),
                    targetUsdt.stripTrailingZeros().toPlainString());

            setStatus(chatId, "✅ Шаг " + step + ": +" + stepDrained.stripTrailingZeros()
                    + " USDT (итого " + drainedUsdt.stripTrailingZeros() + "/" + targetUsdt.stripTrailingZeros() + ")");
            sleepQuiet(STEP_SLEEP_MS);

        }
    }

    /** /stop — жёсткая пауза: снимаем лимитки A/B, сохраняем состояние, монитор останавливаем. */
    public void stopRange(Long chatId) {
        RangeState st = MemoryDb.getRangeState(chatId);
        if (st == null) return;
        final String symbol = st.getSymbol();
        try { spread.stopMonitor(symbol, chatId); } catch (Exception ignore) {}
        try { mexc.cancelAllOpenOrdersAccountA(symbol, chatId); } catch (Exception ignore) {}
        try { mexc.cancelAllOpenOrdersAccountB(symbol, chatId); } catch (Exception ignore) {}
        MemoryDb.updateProgress(chatId, s -> {
            if (s == null) return null;
            s.setPaused(true);
            s.setRunning(false);
            s.setUpdatedAt(Instant.now());
            return s;
        });
        setStatus(chatId, "⏸️ Пауза по /stop. Лимитки сняты, состояние сохранено.");
        log.warn("⏸️ [chat={}] STOP: лимитки сняты, монитор остановлен.", chatId);
    }

    /** /continue <LOW> <HIGH> — продолжить с новой вилкой на остаток цели. */
    public void continueRange(Long chatId, BigDecimal newLow, BigDecimal newHigh) {
        RangeState st = MemoryDb.getRangeState(chatId);
        if (st == null) {
            setStatus(chatId, "⚠️ /continue: нет активного состояния. Сначала /drain в диапазоне.");
            return;
        }
        if (newLow == null || newHigh == null || newLow.compareTo(newHigh) >= 0) {
            setStatus(chatId, "⚠️ /continue: неверные границы диапазона.");
            return;
        }
        final String symbol = st.getSymbol();
        final BigDecimal target = nz(st.getTargetUsdt());
        final BigDecimal drained = nz(st.getDrainedUsdt());
        BigDecimal remaining = target.subtract(drained);
        if (remaining.signum() <= 0) {
            setStatus(chatId, "✅ Нечего продолжать: цель выполнена (" + drained.stripTrailingZeros() + "/" + target.stripTrailingZeros() + ").");
            return;
        }
        try { spread.stopMonitor(symbol, chatId); } catch (Exception ignore) {}
        try { mexc.cancelAllOpenOrdersAccountA(symbol, chatId); } catch (Exception ignore) {}
        try { mexc.cancelAllOpenOrdersAccountB(symbol, chatId); } catch (Exception ignore) {}

        setStatus(chatId, "▶️ CONTINUE " + symbol + " [" + newLow + " .. " + newHigh + "], остаток ~" + remaining.stripTrailingZeros() + " USDT");
        log.info("▶️ [chat={}] CONTINUE {}: новая вилка [{} .. {}], остаток цели ~{} USDT",
                chatId, symbol, newLow.stripTrailingZeros(), newHigh.stripTrailingZeros(), remaining.stripTrailingZeros());

        startDrainInRange(symbol, newLow, newHigh, remaining, chatId, 50);
    }

    /** /status — компактный статус. */
    public String statusText(Long chatId) {
        RangeState s = MemoryDb.getRangeState(chatId);
        if (s == null) return "Статус: нет активного состояния. Используй /drain ...";
        StringBuilder sb = new StringBuilder(256);
        sb.append("📊 Статус RANGE\n");
        sb.append("Символ: ").append(s.getSymbol()).append('\n');
        sb.append("Вилка: [").append(s.getRangeLow()).append(" .. ").append(s.getRangeHigh()).append("]\n");
        sb.append("Прогресс: ").append(nz(s.getDrainedUsdt()).stripTrailingZeros()).append(" / ")
                .append(nz(s.getTargetUsdt()).stripTrailingZeros()).append(" USDT\n");
        sb.append("Шаг: ").append(s.getStep()).append('\n');
        sb.append("Состояние: ").append(s.isPaused() ? "paused" : (s.isRunning() ? "running" : "idle")).append('\n');
        if (s.getUpdatedAt() != null) sb.append("Обновлено: ").append(s.getUpdatedAt()).append('\n');
        try {
            var f = RangeState.class.getDeclaredField("statusText");
            f.setAccessible(true);
            Object val = f.get(s);
            if (val != null) sb.append('\n').append(val);
        } catch (Exception ignore) {}
        return sb.toString();
    }

    // =========================================================================================
    // Внутренние утилиты
    // =========================================================================================

    private static String runId() {
        byte[] b = new byte[4];
        new SecureRandom().nextBytes(b);
        return HexFormat.of().formatHex(b);
    }

    private static BigDecimal nz(BigDecimal v) { return v != null ? v : ZERO; }
    private static int nzInt(Integer v) { return v != null ? v : 0; }

    private static BigDecimal divSafe(BigDecimal a, BigDecimal b) {
        if (a == null || b == null || b.signum() == 0) return ZERO;
        return a.divide(b, 18, RoundingMode.HALF_UP);
    }
    private static BigDecimal floorPositive(BigDecimal v) {
        if (v == null || v.signum() <= 0) return ZERO;
        return v.setScale(0, RoundingMode.FLOOR);
    }
    private static BigDecimal max(BigDecimal a, BigDecimal b) { return a.compareTo(b) >= 0 ? a : b; }

    private boolean pausedNow(Long chatId) {
        RangeState s = MemoryDb.getRangeState(chatId);
        return s != null && s.isPaused();
    }

    /** Помечаем paused + останавливаем монитор. Отмену ордеров делает основной поток/stopRange. */
    private void requestPause(Long chatId, String symbol, String message) {
        log.warn("⏸️ [chat={}] AUTO-PAUSE: {}", chatId, message);
        setStatus(chatId, "⏸️ Пауза: " + message);
        spread.stopMonitor(symbol, chatId);
        MemoryDb.updateProgress(chatId, st -> {
            if (st == null) return null;
            st.setPaused(true);
            st.setRunning(false);
            st.setUpdatedAt(Instant.now());
            return st;
        });
    }

    private void cancelBothSafely(String symbol, Long chatId) {
        try { mexc.cancelAllOpenOrdersAccountA(symbol, chatId); } catch (Exception ignore) {}
        try { mexc.cancelAllOpenOrdersAccountB(symbol, chatId); } catch (Exception ignore) {}
    }

    /** SEED: гарантируем, что на A будет минимум qtyNeed токена. */
    private boolean ensureSeed(String symbol, BigDecimal buyPrice, BigDecimal qtyNeed, Long chatId) {
        BigDecimal balanceA = mexc.getTokenBalanceAccountA(symbol, chatId).stripTrailingZeros();
        if (balanceA.compareTo(qtyNeed) >= 0) return true;

        BigDecimal deficit = qtyNeed.subtract(balanceA);
        if (deficit.signum() <= 0) return true;

        BigDecimal budget = buyPrice.multiply(deficit).multiply(SEED_BUDGET_K);
        try {
            MexcTradeService.OrderInfo m = mexc.marketBuyAccountAFull(symbol, budget, chatId);
            if (m != null && m.executedQty().signum() > 0) return true;
        } catch (Exception ignore) {}

        try {
            MexcTradeService.OrderInfo l = mexc.limitBuyAboveSpreadAccountA(symbol, budget, chatId);
            if (l != null && l.executedQty().signum() > 0) return true;
        } catch (Exception ignore) {}

        return mexc.getTokenBalanceAccountA(symbol, chatId).compareTo(qtyNeed) >= 0;
    }

    /** Пишем короткий статус в RangeState.statusText, если поле есть. */
    private static void setStatus(Long chatId, String text) {
        try {
            MemoryDb.updateProgress(chatId, st -> {
                if (st == null) return null;
                try {
                    var f = RangeState.class.getDeclaredField("statusText");
                    f.setAccessible(true);
                    f.set(st, text);
                } catch (NoSuchFieldException ignore) {
                    // поле может отсутствовать — не критично
                } catch (Exception e) {
                    // игнорируем прочие сбои статуса
                }
                st.setUpdatedAt(Instant.now());
                return st;
            });
        } catch (Exception ignore) { }
    }

    private static void sleepQuiet(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
}
