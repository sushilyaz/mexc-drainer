// src/main/java/com/suhoi/mexcdrainer/service/DrainService.java
package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.DrainSession;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
@Slf4j
@RequiredArgsConstructor
public class DrainService {

    private final MexcTradeService mexcTradeService;
    private final Reconciler reconciler;
    private final AppProperties props;
    private final TelegramService tg; // бин телеги

    // ---------- helpers: форматирование и снимки состояния ----------

    private static String fmt(BigDecimal x) {
        return x == null ? "null" : x.stripTrailingZeros().toPlainString();
    }

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

    /** Унифицированная автопауза + подробный лог в консоль. */
    private BigDecimal autoPauseAndZero(DrainSession s,
                                        DrainSession.AutoPauseReason reason,
                                        String details,
                                        String whereTag) {
        s.autoPause(reason, details);
        log.warn("⏸ AUTO_PAUSE@{} -> reason={} | details={} | {}", whereTag, reason, details, snapshot(s));
        return BigDecimal.ZERO;
    }

    // -----------------------------------------------------------------

    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId, int cycles) {
        var flag = MemoryDb.getFlag(chatId);
        if (!flag.compareAndSet(false, true)) {
            tg.reply(chatId, "⏳ У тебя уже идёт перелив в этом чате.");
            return;
        }

        try {
            log.info("🚀 START_DRAIN: symbol={}, amount={} USDT, cycles={}", symbol, fmt(usdtAmount), cycles);

            // 0) Рынок BUY на A
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

            log.info("SESSION_INIT {}", snapshot(s));
            tg.reply(chatId, "✅ A купил ~%s токенов @avg=%s".formatted(
                    s.getQtyA().stripTrailingZeros(), buyA.avgPrice().stripTrailingZeros()));

            for (int i = 0; i < cycles; i++) {
                s.setCycleIndex(i + 1);
                log.info("===== CYCLE_START #{} {}", s.getCycleIndex(), snapshot(s));
                BigDecimal next = executeCycleWithGuards(chatId, s);
                log.info("===== CYCLE_END   #{} -> nextQtyA={} {}", s.getCycleIndex(), fmt(next), snapshot(s));

                if (s.getState() == DrainSession.State.AUTO_PAUSE) {
                    // Телеграм — как и было, плюс у нас теперь есть консольный WARN из autoPauseAndZero(...)
                    tg.reply(chatId, "⏸ Автопауза: %s – %s".formatted(s.getReason(), s.getReasonDetails()));
                    break;
                }
                if (next == null || next.signum() <= 0) {
                    log.warn("⚠ NEXT_QTY_LE_ZERO: останов. next={}", fmt(next));
                    break;
                }
                s.setQtyA(next);
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

            // === (1) A SELL — рядом с нижней кромкой
            BigDecimal nearSell = mexcTradeService.getNearLowerSpreadPrice(symbol, chatId, cfg.getDepthLimit());
            log.info("[SELL_PLANNED] nearSell={}, planQtyA={}", fmt(nearSell), fmt(s.getQtyA()));

            var placedSell = mexcTradeService.placeLimitSellAccountAPlaced(symbol, nearSell, s.getQtyA(), chatId);
            log.info("[SELL_PLACED] orderId={}, price={}, qty={}",
                    placedSell.orderId(), fmt(placedSell.price()), fmt(placedSell.qty()));

            if (placedSell.orderId() == null || placedSell.qty() == null || placedSell.qty().signum() <= 0) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "SELL не прошёл minNotional/minQty.",
                        "A-SELL-PLACE-FAIL");
            }
            s.setSellOrderId(placedSell.orderId());
            s.setPSell(placedSell.price());
            s.setQtyA(placedSell.qty());
            s.setState(DrainSession.State.A_SELL_PLACED);

            var rqSell = mexcTradeService.ensureTopAskOrRequoteSell(
                    symbol, chatId,
                    s.getSellOrderId(), s.getPSell(), s.getQtyA(),
                    cfg.getMaxRequotesPerLeg(),
                    cfg.getEpsilonTicks(),
                    cfg.getDepthLimit(),
                    cfg.getPostPlaceGraceMs()
            );
            if (!rqSell.ok()) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.UNKNOWN,
                        "ensureTopAskOrRequoteSell -> not ok",
                        "A-SELL-ENSURE");
            }
            s.setSellOrderId(rqSell.orderId());
            s.setPSell(rqSell.price());

            log.info("[SELL_ENSURED] orderId={}, price={}, qty={}",
                    s.getSellOrderId(), fmt(s.getPSell()), fmt(s.getQtyA()));
            log.info("A ➡ SELL лимитка {} токенов @ {} (orderId={})",
                    s.getQtyA().stripTrailingZeros(),
                    s.getPSell().stripTrailingZeros(),
                    s.getSellOrderId());

            // === (2) B MARKET BUY — выкупаем A-SELL
            log.info("[B_BUY_SEND] marketBuyFromAccountB(symbol={}, pSell={}, qtyA={})",
                    symbol, fmt(s.getPSell()), fmt(s.getQtyA()));
            BigDecimal spent = mexcTradeService.marketBuyFromAccountB(symbol, s.getPSell(), s.getQtyA(), chatId, true);
            s.setLastSpentB(spent);
            s.setState(DrainSession.State.B_MKT_BUY_SENT);
            log.info("[B_BUY_DONE] spent={} USDT", fmt(spent));
            log.info("B ➡ BUY market на ~{} USDT", spent == null ? "0" : spent.stripTrailingZeros().toPlainString());

            // Быстрая сверка факта на B
            var vBuyB = reconciler.checkAfterBBuy(symbol, chatId, s);
            log.info("[B_BUY_POSTCHECK] verdict={}", vBuyB);
            if (vBuyB != Reconciler.Verdict.OK) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.PARTIAL_MISMATCH,
                        "После MARKET BUY на B база отсутствует.",
                        "B-BUY-VERIFY");
            }

            // === (3) Ждём FILLED по A-SELL
            var credsA = MemoryDb.getAccountA(chatId);
            var sellAInfo = mexcTradeService.waitUntilFilled(symbol, s.getSellOrderId(), credsA.getApiKey(), credsA.getSecret(), 6000);
            log.info("[A_SELL_FILLED?] status={}, executedQty={}, cummQuote={}, avg={}",
                    sellAInfo.status(), fmt(sellAInfo.executedQty()), fmt(sellAInfo.cummQuoteQty()), fmt(sellAInfo.avgPrice()));

            if (!"FILLED".equals(sellAInfo.status()) || sellAInfo.executedQty().signum() <= 0) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.TIMEOUT,
                        "A SELL не FILLED (status=" + sellAInfo.status() + ")",
                        "A-SELL-WAIT");
            }
            s.setLastCummA(sellAInfo.cummQuoteQty());
            s.setQtyA(sellAInfo.executedQty());
            s.setState(DrainSession.State.A_SELL_FILLED);

            // === (4) A BUY — верхняя кромка
            BigDecimal nearBuy = mexcTradeService.getNearUpperSpreadPrice(symbol, chatId, cfg.getDepthLimit());
            log.info("[BUY_PLANNED] nearBuy={}, lastCummA={}, qtyB_to_sell_likeA={}",
                    fmt(nearBuy), fmt(s.getLastCummA()), fmt(s.getQtyA()));

            BigDecimal plannedSellQtyB = mexcTradeService.planMarketSellQtyAccountB(symbol, nearBuy, s.getQtyA(), chatId);
            log.info("[B_SELL_PLAN] plannedSellQtyB={}", fmt(plannedSellQtyB));
            if (plannedSellQtyB.compareTo(BigDecimal.ZERO) <= 0) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "B не может выставить MARKET SELL ≥ minNotional.",
                        "B-SELL-PLAN");
            }

            BigDecimal spendA = mexcTradeService.reserveForMakerFee(s.getLastCummA());
            BigDecimal capByQty = nearBuy.multiply(plannedSellQtyB);
            if (spendA.compareTo(capByQty) > 0) spendA = capByQty;

            log.info("[BUY_BUDGET] spendA={}, capByQty={}, plannedSellQtyB={}",
                    fmt(spendA), fmt(capByQty), fmt(plannedSellQtyB));

            var placedBuy = mexcTradeService.placeLimitBuyAccountAPlaced(symbol, nearBuy, spendA, plannedSellQtyB, chatId);
            log.info("[BUY_PLACED] orderId={}, price={}, qty={} (requestedBudget={}, requestedMaxQty={})",
                    placedBuy.orderId(), fmt(placedBuy.price()), fmt(placedBuy.qty()), fmt(spendA), fmt(plannedSellQtyB));

            if (placedBuy.orderId() == null || placedBuy.qty() == null || placedBuy.qty().signum() <= 0) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "BUY не прошёл minNotional/minQty.",
                        "A-BUY-PLACE-FAIL");
            }
            s.setBuyOrderId(placedBuy.orderId());
            s.setPBuy(placedBuy.price());
            plannedSellQtyB = placedBuy.qty(); // синхроним объём B SELL под фактический BUY qty

            var rqBuy = mexcTradeService.ensureTopBidOrRequoteBuy(
                    symbol, chatId,
                    s.getBuyOrderId(), s.getPBuy(),
                    spendA, plannedSellQtyB,
                    cfg.getMaxRequotesPerLeg(),
                    cfg.getEpsilonTicks(),
                    cfg.getDepthLimit(),
                    cfg.getPostPlaceGraceMs()
            );
            if (!rqBuy.ok()) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.UNKNOWN,
                        "ensureTopBidOrRequoteBuy -> not ok",
                        "A-BUY-ENSURE");
            }
            s.setBuyOrderId(rqBuy.orderId());
            s.setPBuy(rqBuy.price());

            log.info("[BUY_ENSURED] orderId={}, price={}, plannedBsellQty={}",
                    s.getBuyOrderId(), fmt(s.getPBuy()), fmt(plannedSellQtyB));
            log.info("A ➡ BUY лимитка {} USDT @ {} (maxQty={} ; orderId={})",
                    fmt(spendA),
                    s.getPBuy().stripTrailingZeros(),
                    plannedSellQtyB.stripTrailingZeros(),
                    s.getBuyOrderId());

            var vBuyPlaced = reconciler.checkAfterBuyPlaced(symbol, chatId, s);
            log.info("[BUY_POSTCHECK] verdict={}", vBuyPlaced);
            if (vBuyPlaced != Reconciler.Verdict.OK) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.FRONT_RUN,
                        "После A-BUY наш bid не топ (верификация).",
                        "A-BUY-VERIFY");
            }

            // === (5) B MARKET SELL — под фактический BUY qty
            log.info("[B_SELL_SEND] marketSellFromAccountB(symbol={}, pBuy={}, qty={})",
                    symbol, fmt(s.getPBuy()), fmt(plannedSellQtyB));
            mexcTradeService.marketSellFromAccountB(symbol, s.getPBuy(), plannedSellQtyB, chatId);
            s.setState(DrainSession.State.B_MKT_SELL_SENT);
            log.info("[B_SELL_SENT] ok");

            var vSellB = reconciler.checkAfterBSell(symbol, chatId, s);
            log.info("[B_SELL_POSTCHECK] verdict={}", vSellB);
            if (vSellB != Reconciler.Verdict.OK) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.PARTIAL_MISMATCH,
                        "После MARKET SELL на B осталась не-пыль.",
                        "B-SELL-VERIFY");
            }

            // === (6) Ждём FILLED по A-BUY — это next qty
            var credsA2 = MemoryDb.getAccountA(chatId);
            var buyAInfo = mexcTradeService.waitUntilFilled(symbol, s.getBuyOrderId(), credsA2.getApiKey(), credsA2.getSecret(), 6000);
            log.info("[A_BUY_FILLED?] status={}, executedQty={}, cummQuote={}, avg={}",
                    buyAInfo.status(), fmt(buyAInfo.executedQty()), fmt(buyAInfo.cummQuoteQty()), fmt(buyAInfo.avgPrice()));

            if (!"FILLED".equals(buyAInfo.status()) || buyAInfo.executedQty().signum() <= 0) {
                return autoPauseAndZero(s,
                        DrainSession.AutoPauseReason.TIMEOUT,
                        "A BUY не FILLED (status=" + buyAInfo.status() + ")",
                        "A-BUY-WAIT");
            }

            long dt = System.currentTimeMillis() - tCycle;
            log.info("✅ Цикл {} завершён за {} ms. A получил {} токенов (avg={}), потратил {} USDT.",
                    s.getCycleIndex(),
                    dt,
                    buyAInfo.executedQty().stripTrailingZeros().toPlainString(),
                    buyAInfo.avgPrice().stripTrailingZeros().toPlainString(),
                    buyAInfo.cummQuoteQty().stripTrailingZeros().toPlainString());

            s.setState(DrainSession.State.A_BUY_FILLED);
            return buyAInfo.executedQty();

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

    // Ручная пауза
    public void requestStop(Long chatId) {
        var s = MemoryDb.getSession(chatId);
        if (s == null) return;
        s.autoPause(DrainSession.AutoPauseReason.MANUAL, "Остановлено пользователем.");
        log.warn("🛑 MANUAL_STOP {}", snapshot(s));
    }

    // Продолжение из факта балансов
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
            log.info("CONTINUE_SESSION_INIT {}", snapshot(s));

            for (int i = 0; i < cycles; i++) {
                s.setCycleIndex(i + 1);
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
                s.getState(), s.getCycleIndex(),
                s.getPSell(), s.getPBuy(),
                s.getReason(), s.getReasonDetails());
    }
}
