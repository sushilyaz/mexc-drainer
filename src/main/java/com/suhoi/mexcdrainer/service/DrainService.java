package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.DrainSession;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Service
@Slf4j
@RequiredArgsConstructor
public class DrainService {

    private static final boolean FAST_CROSS_IOC = true;   // быстрый выкуп через LIMIT IOC на B сразу после A-SELL
    private static final int FAST_ENSURE_GRACE_MS = 150;  // «быстрый» grace для ensure после неудачного IOC
    private static final int FAST_MAX_REQUOTES = 1;       // максимум перестановок в «быстром» сценарии

    private final MexcTradeWsService ws;                 // фасад WS
    private final MexcTradeService mexcTradeService;     // REST и бизнес-операции
    private final Reconciler reconciler;
    private final AppProperties props;
    private final TelegramService tg;

    // ---------- helpers ----------
    private static String fmt(BigDecimal x) { return x == null ? "null" : x.stripTrailingZeros().toPlainString(); }

    private String snapshot(DrainSession s) {
        if (s == null) return "{session=null}";
        return new StringBuilder(256)
                .append("{state=").append(s.getState())
                .append(", cycle=").append(s.getCycleIndex())
                .append(", symbol=").append(s.getSymbol())
                .append(", qtyA=").append(fmt(s.getQtyA()))
                .append(", pSell=").append(fmt(s.getPSell()))
                .append(", pBuy=").append(fmt(s.getPBuy()))
                .append(", lastSpentB=").append(fmt(s.getLastSpentB()))
                .append(", lastCummA=").append(fmt(s.getLastCummA()))
                .append(", sellOrderId=").append(s.getSellOrderId())
                .append(", buyOrderId=").append(s.getBuyOrderId())
                .append(", reason=").append(s.getReason())
                .append(", details=").append(s.getReasonDetails())
                .append('}')
                .toString();
    }

    private BigDecimal autoPauseAndZero(DrainSession s, DrainSession.AutoPauseReason reason, String details, String whereTag) {
        s.autoPause(reason, details);
        log.warn("⏸ AUTO_PAUSE@{} -> reason={} | details={} | {}", whereTag, reason, details, snapshot(s));
        return BigDecimal.ZERO;
    }

    // ---------- API ----------

    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId, int cycles) {
        var flag = MemoryDb.getFlag(chatId);
        if (!flag.compareAndSet(false, true)) {
            tg.reply(chatId, "⏳ У тебя уже идёт перелив в этом чате.");
            return;
        }

        try {
            log.info("🚀 START_DRAIN: symbol={}, amount={} USDT, cycles={}", symbol, fmt(usdtAmount), cycles);

            // 0) MKT BUY на A
            var buyA = mexcTradeService.marketBuyAccountAFull(symbol, usdtAmount, chatId);
            log.info("A_MKT_BUY_RESULT: status={}, executedQty={}, cummQuote={}, avg={}",
                    buyA == null ? "null" : buyA.status(),
                    buyA == null ? "null" : fmt(buyA.executedQty()),
                    buyA == null ? "null" : fmt(buyA.cummQuoteQty()),
                    buyA == null ? "null" : fmt(buyA.avgPrice()));

            if (buyA == null || buyA.executedQty().signum() <= 0) {
                log.error("❌ A_MKT_BUY_EMPTY: status={}", buyA == null ? "null" : buyA.status());
                return;
            }

            var s = new DrainSession();
            s.setSymbol(symbol);
            s.setState(DrainSession.State.A_MKT_BUY_DONE);
            s.setQtyA(buyA.executedQty());
            MemoryDb.setSession(chatId, s);

            // готовим WS (приватные сокеты A/B + подписка на bookTicker)
            ws.prepareWs(chatId, symbol);

            log.info("SESSION_INIT {}", snapshot(s));
            tg.reply(chatId, "✅ A купил ~%s токенов @avg=%s".formatted(
                    s.getQtyA().stripTrailingZeros(), buyA.avgPrice().stripTrailingZeros()));

            int i = 0;
            boolean unlimited = (cycles <= 0);
            while (unlimited || i < cycles) {
                s.setCycleIndex(++i);
                log.info("===== CYCLE_START #{} {}", s.getCycleIndex(), snapshot(s));

                BigDecimal next = executeCycleWithGuards(chatId, s);
                log.info("===== CYCLE_END   #{} -> nextQtyA={} {}", s.getCycleIndex(), fmt(next), snapshot(s));

                if (s.getState() == DrainSession.State.AUTO_PAUSE) {
                    tg.reply(chatId, "⏸ Автопауза: %s – %s".formatted(s.getReason(), s.getReasonDetails()));
                    break;
                }
                if (next == null || next.signum() <= 0) {
                    log.warn("⚠ NEXT_QTY_LE_ZERO: останов. next={}", fmt(next));
                    break;
                }
                s.setQtyA(next);
            }

            var sEnd = MemoryDb.getSession(chatId);
            if (sEnd != null && !(sEnd.getState() == DrainSession.State.AUTO_PAUSE
                    && sEnd.getReason() == DrainSession.AutoPauseReason.MANUAL)) {
                finalSweepSellIfPossible(chatId, symbol);
            }

        } catch (Exception e) {
            log.error("❌ Ошибка в startDrain", e);
        } finally {
            MemoryDb.getFlag(chatId).set(false);
            log.info("🏁 STOP_DRAIN: symbol={}, chatId={}", symbol, chatId);
        }
    }

    private BigDecimal executeCycleWithGuards(Long chatId, DrainSession s) {
        final String symbol = s.getSymbol();

        try {
            long tCycle = System.currentTimeMillis();
            var cfg = props.getDrain();

            // (1) A SELL — около нижней кромки
            var f = mexcTradeService.getSymbolFilters(symbol);

            BigDecimal nearSell = mexcTradeService.getNearLowerSpreadPrice(symbol, chatId, cfg.getDepthLimit());
            BigDecimal minQtyForSell = minQtyForNotional(nearSell, f);

            if (s.getQtyA() == null || s.getQtyA().compareTo(minQtyForSell) < 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "qtyA < minQtyForNotional для SELL @ " + fmt(nearSell) + " (qtyA=" + fmt(s.getQtyA()) + ", min=" + fmt(minQtyForSell) + ")",
                        "PRE-A-SELL-MIN");
            }

            log.info("[SELL_PLANNED] nearSell={}, planQtyA={}", fmt(nearSell), fmt(s.getQtyA()));

            var placedSell = mexcTradeService.placeLimitSellAccountAPlaced(symbol, nearSell, s.getQtyA(), chatId);
            log.info("[SELL_PLACED] orderId={}, price={}, qty={}",
                    placedSell.orderId(), fmt(placedSell.price()), fmt(placedSell.qty()));

            if (placedSell.orderId() == null || placedSell.qty() == null || placedSell.qty().signum() <= 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "SELL не прошёл minNotional/minQty.", "A-SELL-PLACE-FAIL");
            }
            s.setSellOrderId(placedSell.orderId());
            s.setPSell(placedSell.price());
            s.setQtyA(placedSell.qty());
            s.setState(DrainSession.State.A_SELL_PLACED);

            // (1a) FAST PATH: сразу B LIMIT IOC BUY
            if (FAST_CROSS_IOC) {
                ws.awaitNextTick(symbol, Math.min(120, props.getDrain().getWs().getMaxStalenessMs()));

                log.info("[B_BUY_SEND_FAST_IOC] limitIocBuyFromAccountB(symbol={}, price={}, qty={})",
                        symbol, fmt(s.getPSell()), fmt(s.getQtyA()));
                mexcTradeService.limitIocBuyFromAccountB(symbol, s.getPSell(), s.getQtyA(), chatId);
                s.setLastSpentB(s.getPSell().multiply(s.getQtyA()));
                s.setState(DrainSession.State.B_MKT_BUY_SENT);
                log.info("[B_BUY_FAST_IOC_SENT] approxSpent={} USDT", fmt(s.getLastSpentB()));

                var vBuyB_fast = reconciler.checkAfterBBuy(symbol, chatId, s);
                log.info("[B_BUY_FAST_POSTCHECK] verdict={}", vBuyB_fast);

                if (vBuyB_fast != Reconciler.Verdict.OK) {
                    var rqSell = mexcTradeService.ensureTopAskOrRequoteSell(
                            symbol, chatId,
                            s.getSellOrderId(), s.getPSell(), s.getQtyA(),
                            Math.min(FAST_MAX_REQUOTES, cfg.getMaxRequotesPerLeg()),
                            cfg.getEpsilonTicks(),
                            cfg.getDepthLimit(),
                            Math.min(FAST_ENSURE_GRACE_MS, cfg.getPostPlaceGraceMs())
                    );
                    if (!rqSell.ok()) {
                        return autoPauseAndZero(s, DrainSession.AutoPauseReason.UNKNOWN,
                                "ensureTopAskOrRequoteSell (fast) -> not ok", "A-SELL-ENSURE-FAST");
                    }
                    s.setSellOrderId(rqSell.orderId());
                    s.setPSell(rqSell.price());
                    log.info("[SELL_ENSURED_FAST] orderId={}, price={}, qty={}",
                            s.getSellOrderId(), fmt(s.getPSell()), fmt(s.getQtyA()));

                    log.info("[B_BUY_SEND_FALLBACK_MKT] marketBuyFromAccountB(symbol={}, pSell={}, qtyA={})",
                            symbol, fmt(s.getPSell()), fmt(s.getQtyA()));
                    BigDecimal spent = mexcTradeService.marketBuyFromAccountB(symbol, s.getPSell(), s.getQtyA(), chatId, true);
                    s.setLastSpentB(spent);
                    s.setState(DrainSession.State.B_MKT_BUY_SENT);
                    log.info("[B_BUY_DONE_FALLBACK] spent={} USDT", fmt(spent));

                    var vBuyB_fb = reconciler.checkAfterBBuy(symbol, chatId, s);
                    log.info("[B_BUY_POSTCHECK] verdict={}", vBuyB_fb);
                    if (vBuyB_fb != Reconciler.Verdict.OK) {
                        return autoPauseAndZero(s, DrainSession.AutoPauseReason.PARTIAL_MISMATCH,
                                "После MARKET BUY на B база отсутствует.", "B-BUY-VERIFY");
                    }
                }
            } else {
                var rqSell = mexcTradeService.ensureTopAskOrRequoteSell(
                        symbol, chatId,
                        s.getSellOrderId(), s.getPSell(), s.getQtyA(),
                        cfg.getMaxRequotesPerLeg(),
                        cfg.getEpsilonTicks(),
                        cfg.getDepthLimit(),
                        cfg.getPostPlaceGraceMs()
                );
                if (!rqSell.ok()) {
                    return autoPauseAndZero(s, DrainSession.AutoPauseReason.UNKNOWN,
                            "ensureTopAskOrRequoteSell -> not ok", "A-SELL-ENSURE");
                }
                s.setSellOrderId(rqSell.orderId());
                s.setPSell(rqSell.price());

                log.info("[SELL_ENSURED] orderId={}, price={}, qty={}",
                        s.getSellOrderId(), fmt(s.getPSell()), fmt(s.getQtyA()));

                log.info("[B_BUY_SEND] marketBuyFromAccountB(symbol={}, pSell={}, qtyA={})",
                        symbol, fmt(s.getPSell()), fmt(s.getQtyA()));
                BigDecimal spent = mexcTradeService.marketBuyFromAccountB(symbol, s.getPSell(), s.getQtyA(), chatId, true);
                s.setLastSpentB(spent);
                s.setState(DrainSession.State.B_MKT_BUY_SENT);
                log.info("[B_BUY_DONE] spent={} USDT", fmt(spent));

                var vBuyB = reconciler.checkAfterBBuy(symbol, chatId, s);
                log.info("[B_BUY_POSTCHECK] verdict={}", vBuyB);
                if (vBuyB != Reconciler.Verdict.OK) {
                    return autoPauseAndZero(s, DrainSession.AutoPauseReason.PARTIAL_MISMATCH,
                            "После MARKET BUY на B база отсутствует.", "B-BUY-VERIFY");
                }
            }

            // (3) Ждём FILLED по A-SELL (приватный WS)
            var sellUpd = ws.awaitOrderFilledA(symbol, s.getSellOrderId(), 6000, chatId).join();
            log.info("[A_SELL_FILLED?] status={} cumQty={} avg={}",
                    sellUpd.getStatus(), sellUpd.getCumulativeQuantity(), sellUpd.getAvgPrice());
            if (!sellUpd.isFilled() || sellUpd.getCumulativeQuantity() == null || sellUpd.getCumulativeQuantity().signum() <= 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.TIMEOUT, "A SELL не FILLED (WS)", "A-SELL-WAIT");
            }
            s.setLastCummA(sellUpd.getCumulativeQuantity().multiply(sellUpd.getAvgPrice())); // ≈ cummQuote
            s.setQtyA(sellUpd.getCumulativeQuantity());
            s.setState(DrainSession.State.A_SELL_FILLED);

            // (4) A BUY — верхняя кромка
            BigDecimal nearBuy = mexcTradeService.getNearUpperSpreadPrice(symbol, chatId, cfg.getDepthLimit());
            log.info("[BUY_PLANNED] nearBuy={}, lastCummA={}, qtyB_to_sell_likeA={}",
                    fmt(nearBuy), fmt(s.getLastCummA()), fmt(s.getQtyA()));

            BigDecimal plannedSellQtyB = mexcTradeService.planMarketSellQtyAccountB(symbol, nearBuy, s.getQtyA(), chatId);
            log.info("[B_SELL_PLAN] plannedSellQtyB={}", fmt(plannedSellQtyB));
            if (plannedSellQtyB.compareTo(BigDecimal.ZERO) <= 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "B не может выставить MARKET SELL ≥ minNotional.", "B-SELL-PLAN");
            }

            BigDecimal spendA = mexcTradeService.reserveForMakerFee(s.getLastCummA());
            BigDecimal capByQty = nearBuy.multiply(plannedSellQtyB);
            if (spendA.compareTo(capByQty) > 0) spendA = capByQty;

            log.info("[BUY_BUDGET] spendA={}, capByQty={}, plannedSellQtyB={}",
                    fmt(spendA), fmt(capByQty), fmt(plannedSellQtyB));

            // стоп-условие minNotional
            if (f != null && f.minNotional != null && f.minNotional.signum() > 0 && spendA.compareTo(f.minNotional) < 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "spendA < minNotional для BUY (spendA=" + fmt(spendA) + ", minNotional=" + fmt(f.minNotional) + ")",
                        "PRE-A-BUY-MIN");
            }

            var placedBuy = mexcTradeService.placeLimitBuyAccountAPlaced(symbol, nearBuy, spendA, plannedSellQtyB, chatId);
            log.info("[BUY_PLACED] orderId={}, price={}, qty={} (requestedBudget={}, requestedMaxQty={})",
                    placedBuy.orderId(), fmt(placedBuy.price()), fmt(placedBuy.qty()), fmt(spendA), fmt(plannedSellQtyB));

            if (placedBuy.orderId() == null || placedBuy.qty() == null || placedBuy.qty().signum() <= 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "BUY не прошёл minNotional/minQty.", "A-BUY-PLACE-FAIL");
            }
            s.setBuyOrderId(placedBuy.orderId());
            s.setPBuy(placedBuy.price());
            plannedSellQtyB = placedBuy.qty();
            s.setPlannedSellQtyB(plannedSellQtyB);

            log.info("[BUY_PLACED_SYNC] orderId={}, price={}, plannedBsellQty={} (expected B remainder ≈ {})",
                    s.getBuyOrderId(), fmt(s.getPBuy()), fmt(plannedSellQtyB),
                    fmt(s.getQtyA().subtract(plannedSellQtyB)));

            // (4a) FAST CROSS: сразу LIMIT IOC SELL на B
            boolean fastSellOk = false;
            if (FAST_CROSS_IOC) {
                ws.awaitNextTick(symbol, Math.min(120, props.getDrain().getWs().getMaxStalenessMs()));

                log.info("[B_SELL_SEND_FAST_IOC] limitSellBelowSpreadAccountB(symbol={}, qty={})",
                        symbol, fmt(plannedSellQtyB));

                s.setBBaseBeforeSell(mexcTradeService.getTokenBalanceAccountB(symbol, chatId));
                log.info("[B_SELL_PRECHECK] bBaseBeforeSell={} (will sell={})",
                        fmt(s.getBBaseBeforeSell()), fmt(plannedSellQtyB));

                mexcTradeService.limitSellBelowSpreadAccountB(symbol, plannedSellQtyB, chatId);
                s.setState(DrainSession.State.B_MKT_SELL_SENT);

                fastSellOk = (reconciler.checkAfterBSell(symbol, chatId, s) == Reconciler.Verdict.OK);
                log.info("[B_SELL_FAST_POSTCHECK] ok={}", fastSellOk);

                if (!fastSellOk) {
                    try {
                        mexcTradeService.cancelOrderAccountA(symbol, s.getBuyOrderId(), chatId);
                        log.warn("[A-BUY-CANCELLED] buyOrderId={} из-за неуспешного B IOC SELL", s.getBuyOrderId());
                    } catch (Exception e) {
                        log.warn("[A-BUY-CANCEL-FAIL] orderId={} err={}", s.getBuyOrderId(), e.getMessage());
                    }
                    return autoPauseAndZero(s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                            "B IOC SELL не выполнен (вероятно notional < minNotional).", "B-SELL-IOC-FAST-FAIL");
                }
            }

            int ensureGrace = Math.min(FAST_ENSURE_GRACE_MS, cfg.getPostPlaceGraceMs());
            var rqBuy = mexcTradeService.ensureTopBidOrRequoteBuy(
                    symbol, chatId,
                    s.getBuyOrderId(), s.getPBuy(),
                    spendA, plannedSellQtyB,
                    cfg.getMaxRequotesPerLeg(),
                    cfg.getEpsilonTicks(),
                    cfg.getDepthLimit(),
                    ensureGrace
            );
            if (!rqBuy.ok()) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.UNKNOWN,
                        "ensureTopBidOrRequoteBuy -> not ok", "A-BUY-ENSURE");
            }
            s.setBuyOrderId(rqBuy.orderId());
            s.setPBuy(rqBuy.price());

            log.info("[BUY_ENSURED] orderId={}, price={}, plannedBsellQty={}",
                    s.getBuyOrderId(), fmt(s.getPBuy()), fmt(plannedSellQtyB));

            var vBuyPlaced = reconciler.checkAfterBuyPlaced(symbol, chatId, s);
            log.info("[BUY_POSTCHECK] verdict={}", vBuyPlaced);
            if (vBuyPlaced != Reconciler.Verdict.OK) {
                var rq2 = mexcTradeService.ensureTopBidOrRequoteBuy(
                        symbol, chatId,
                        s.getBuyOrderId(), s.getPBuy(),
                        spendA, plannedSellQtyB,
                        1, cfg.getEpsilonTicks(), cfg.getDepthLimit(), 40
                );
                if (!rq2.ok()) {
                    return autoPauseAndZero(s, DrainSession.AutoPauseReason.FRONT_RUN,
                            "После A-BUY нас подрезали повторно.", "A-BUY-RECHECK");
                }
                s.setBuyOrderId(rq2.orderId());
                s.setPBuy(rq2.price());
                log.info("[BUY_RECHECK_OK] orderId={}, price={}", s.getBuyOrderId(), fmt(s.getPBuy()));
            }

            // (5) Если FAST выключен — fallback-секция (опущено, т.к. FAST_CROSS_IOC=true)

            // (6) Ждём FILLED по A-BUY (это next qty)
            var buyUpd = ws.awaitOrderFilledA(symbol, s.getBuyOrderId(), 6000, chatId).join();
            log.info("[A_BUY_FILLED?] status={} cumQty={} avg={}",
                    buyUpd.getStatus(), buyUpd.getCumulativeQuantity(), buyUpd.getAvgPrice());
            if (!buyUpd.isFilled() || buyUpd.getCumulativeQuantity() == null || buyUpd.getCumulativeQuantity().signum() <= 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.TIMEOUT, "A BUY не FILLED (WS)", "A-BUY-WAIT");
            }

            long dt = System.currentTimeMillis() - tCycle;
            log.info("✅ Цикл {} завершён за {} ms. A получил {} токенов (avg={})",
                    s.getCycleIndex(), dt,
                    buyUpd.getCumulativeQuantity().stripTrailingZeros().toPlainString(),
                    buyUpd.getAvgPrice().stripTrailingZeros().toPlainString());

            s.setState(DrainSession.State.A_BUY_FILLED);
            return buyUpd.getCumulativeQuantity();

        } catch (Exception e) {
            log.error("❌ Ошибка в executeCycleWithGuards: {} | {}", e.getClass().getSimpleName(), e.getMessage(), e);
            log.error("SESSION_ON_EXCEPTION {}", snapshot(s));
            try {
                BigDecimal tokensA = mexcTradeService.getTokenBalanceAccountA(s.getSymbol(), chatId);
                log.warn("FORCE_SELL_ATTEMPT: balanceA={} {}", fmt(tokensA), s.getSymbol());
                if (tokensA.compareTo(BigDecimal.ZERO) > 0) {
                    mexcTradeService.forceMarketSellAccountA(s.getSymbol(), tokensA, chatId);
                    log.warn("FORCE_SELL_DONE");
                }
            } catch (Exception ex) {
                log.error("Не удалось аварийно продать остаток A: {}", ex.getMessage(), ex);
            }
            s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, e.getClass().getSimpleName());
            log.warn("⏸ AUTO_PAUSE@EXCEPTION {}", snapshot(s));
            return BigDecimal.ZERO;
        }
    }

    public void requestStop(Long chatId) {
        var s = MemoryDb.getSession(chatId);
        if (s == null) return;
        s.autoPause(DrainSession.AutoPauseReason.MANUAL, "Остановлено пользователем.");
        log.warn("🛑 MANUAL_STOP {}", snapshot(s));
    }

    public void continueFromBalances(String symbol, Long chatId, int cycles) {
        var flag = MemoryDb.getFlag(chatId);
        if (!flag.compareAndSet(false, true)) {
            tg.reply(chatId, "⏳ Уже идёт перелив в этом чате.");
            return;
        }
        try {
            var s = new DrainSession();
            s.setSymbol(symbol);

            var f = mexcTradeService.getSymbolFilters(symbol);
            BigDecimal aBase = mexcTradeService.getTokenBalanceAccountA(symbol, chatId);
            log.info("CONTINUE_FROM_BALANCES: aBase={}, minQty={}", fmt(aBase), fmt(f.minQty));
            if (aBase.compareTo(f.minQty) < 0) {
                tg.reply(chatId, "❌ На A мало базового токена для продолжения (нужно ≥ minQty).");
                log.warn("CONTINUE_ABORT: insufficient A base. {}", snapshot(s));
                return;
            }
            s.setQtyA(aBase);
            s.setState(DrainSession.State.A_MKT_BUY_DONE);
            MemoryDb.setSession(chatId, s);

            // готовим WS
            ws.prepareWs(chatId, symbol);

            int i = 0;
            boolean unlimited = (cycles <= 0);
            while (unlimited || i < cycles) {
                s.setCycleIndex(++i);
                log.info("===== CYCLE_START #{} {}", s.getCycleIndex(), snapshot(s));
                BigDecimal next = executeCycleWithGuards(chatId, s);
                log.info("===== CYCLE_END   #{} -> nextQtyA={} {}", s.getCycleIndex(), fmt(next), snapshot(s));

                if (s.getState() == DrainSession.State.AUTO_PAUSE) {
                    tg.reply(chatId, "⏸ Автопауза: %s – %s".formatted(s.getReason(), s.getReasonDetails()));
                    break;
                }
                if (next == null || next.signum() <= 0) break;
                s.setQtyA(next);
            }
        } finally {
            MemoryDb.getFlag(chatId).set(false);
            log.info("🏁 STOP_CONTINUE: symbol={}, chatId={}", symbol, chatId);
        }
    }

    public String status(Long chatId) {
        var s = MemoryDb.getSession(chatId);
        if (s == null) return "Статус: нет активной сессии.";
        return "Статус: %s | цикл %d | pSell=%s | pBuy=%s | reason=%s (%s)".formatted(
                s.getState(), s.getCycleIndex(), s.getPSell(), s.getPBuy(), s.getReason(), s.getReasonDetails());
    }

    // --- utils ---

    /** Минимальное кол-во base, чтобы пройти minNotional/minQty при цене price. */
    private BigDecimal minQtyForNotional(BigDecimal price, MexcTradeService.SymbolFilters f) {
        if (f == null) return BigDecimal.ZERO;
        BigDecimal minQty = (f.minQty != null) ? f.minQty : BigDecimal.ZERO;

        if (f.minNotional == null || f.minNotional.signum() <= 0 || price == null || price.signum() <= 0) {
            return minQty;
        }

        BigDecimal raw = f.minNotional.divide(price, 16, RoundingMode.UP);
        if (f.stepSize != null && f.stepSize.signum() > 0) {
            BigDecimal steps = raw.divide(f.stepSize, 0, RoundingMode.UP);
            return steps.multiply(f.stepSize).max(minQty);
        }
        return raw.max(minQty);
    }

    /** Финальный свип с MARKET SELL на A (если проходим минимумы). */
    private void finalSweepSellIfPossible(Long chatId, String symbol) {
        try {
            var f = mexcTradeService.getSymbolFilters(symbol);
            BigDecimal qtyA = mexcTradeService.getTokenBalanceAccountA(symbol, chatId);
            if (qtyA == null || qtyA.signum() <= 0) {
                log.info("FINAL_SWEEP_SKIP: пусто на A");
                return;
            }
            BigDecimal nearSell = mexcTradeService.getNearLowerSpreadPrice(symbol, chatId, props.getDrain().getDepthLimit());
            BigDecimal minQtyForSell = minQtyForNotional(nearSell, f);

            if (qtyA.compareTo(minQtyForSell) >= 0) {
                log.info("FINAL_SWEEP_SELL: qtyA={} >= minQtyForSell={} -> MARKET SELL", fmt(qtyA), fmt(minQtyForSell));
                mexcTradeService.forceMarketSellAccountA(symbol, qtyA, chatId);
                tg.reply(chatId, "🧹 Финальный SELL: продал остаток %s %s".formatted(
                        qtyA.stripTrailingZeros().toPlainString(), symbol.replace("USDT", "")));
            } else {
                log.info("FINAL_SWEEP_SKIP: qtyA={} < minQtyForSell={} (пыль).", fmt(qtyA), fmt(minQtyForSell));
            }
        } catch (Exception e) {
            log.error("FINAL_SWEEP_ERROR: {}", e.getMessage(), e);
        }
    }
}
