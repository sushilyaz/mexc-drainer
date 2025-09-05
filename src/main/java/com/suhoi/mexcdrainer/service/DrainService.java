// src/main/java/com/suhoi/mexcdrainer/service/DrainService.java
package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.DrainSession;
import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

import static java.lang.Thread.sleep;

@Service
@Slf4j
@RequiredArgsConstructor
public class DrainService {

    private final MexcTradeService mexcTradeService;
    private final Reconciler reconciler;
    private final AppProperties props;
    private final TelegramService tg; // добавь бин

    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId, int cycles) {
        var flag = MemoryDb.getFlag(chatId);
        if (!flag.compareAndSet(false, true)) {
            tg.reply(chatId, "⏳ У тебя уже идёт перелив в этом чате.");
            return;
        }

        try {
            log.info("🚀 Запуск перелива: символ={}, сумма={} USDT", symbol, usdtAmount);

            // STATE: покупка рынка на A — как было
            var buyA = mexcTradeService.marketBuyAccountAFull(symbol, usdtAmount, chatId);
            if (buyA == null || buyA.executedQty().signum() <= 0) {
                log.error("A ➡ Market BUY не дал executedQty. Статус={}", buyA == null ? "null" : buyA.status());
                return;
            }

            var s = new DrainSession();
            s.setSymbol(symbol);
            s.setState(DrainSession.State.A_MKT_BUY_DONE);
            s.setQtyA(buyA.executedQty());
            MemoryDb.setSession(chatId, s);

            tg.reply(chatId, "✅ A купил ~%s токенов @avg=%s".formatted(
                    s.getQtyA().stripTrailingZeros(), buyA.avgPrice().stripTrailingZeros()));

            for (int i = 0; i < cycles; i++) {
                s.setCycleIndex(i + 1);
                BigDecimal next = executeCycleWithGuards(chatId, s);
                if (s.getState() == DrainSession.State.AUTO_PAUSE) {
                    tg.reply(chatId, "⏸ Автопауза: %s – %s".formatted(s.getReason(), s.getReasonDetails()));
                    break;
                }
                if (next == null || next.signum() <= 0) {
                    log.warn("⚠ Следующий объём для цикла <= 0 — останавливаемся");
                    break;
                }
                s.setQtyA(next);
            }

        } catch (Exception e) {
            log.error("❌ Ошибка в startDrain", e);
        } finally {
            MemoryDb.getFlag(chatId).set(false);
        }
    }

    private BigDecimal executeCycleWithGuards(Long chatId, DrainSession s) {
        final String symbol = s.getSymbol();

        try {
            long tCycle = System.currentTimeMillis();

            // === A SELL возле нижней кромки (ex-self)
            BigDecimal sellPrice = mexcTradeService.getNearLowerSpreadPrice(
                    symbol, chatId, props.getDrain().getDepthLimit());
            s.setPSell(sellPrice);

            String sellOrderId = mexcTradeService.placeLimitSellAccountA(symbol, sellPrice, s.getQtyA(), chatId);
            s.setSellOrderId(sellOrderId);
            s.setState(DrainSession.State.A_SELL_PLACED);

            var cfg = props.getDrain();
            var rqSell = mexcTradeService.ensureTopAskOrRequoteSell(
                    symbol, chatId,
                    s.getSellOrderId(), s.getPSell(), s.getQtyA(),
                    cfg.getMaxRequotesPerLeg(),
                    cfg.getEpsilonTicks(),
                    cfg.getDepthLimit(),
                    cfg.getPostPlaceGraceMs()
            );
            if (!rqSell.ok()) {
                s.setState(DrainSession.State.AUTO_PAUSE);
                return BigDecimal.ZERO;
            }
            s.setSellOrderId(rqSell.orderId());
            s.setPSell(rqSell.price());

            log.info("A ➡ SELL лимитка {} токенов @ {} (orderId={})",
                    s.getQtyA().stripTrailingZeros(),
                    s.getPSell().stripTrailingZeros(),
                    s.getSellOrderId());

            // === B MARKET BUY (вернём фактически потраченный quote)
            BigDecimal spent = mexcTradeService.marketBuyFromAccountB(symbol, s.getPSell(), s.getQtyA(), chatId, true);
            s.setLastSpentB(spent);
            s.setState(DrainSession.State.B_MKT_BUY_SENT);
            log.info("B ➡ BUY market на ~{} USDT", spent == null ? "0" : spent.stripTrailingZeros().toPlainString());

            // Быстрая сверка факта на B
            var vBuy = reconciler.checkAfterBBuy(symbol, chatId, s);
            if (vBuy != Reconciler.Verdict.OK) {
                s.autoPause(DrainSession.AutoPauseReason.PARTIAL_MISMATCH, "После MARKET BUY на B база отсутствует.");
                return BigDecimal.ZERO;
            }

            // === Ждём FILLED по A-SELL
            var credsA = MemoryDb.getAccountA(chatId);
            var sellAInfo = mexcTradeService.waitUntilFilled(symbol, s.getSellOrderId(), credsA.getApiKey(), credsA.getSecret(), 6000);
            if (!"FILLED".equals(sellAInfo.status()) || sellAInfo.executedQty().signum() <= 0) {
                s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "A SELL не FILLED (status=" + sellAInfo.status() + ")");
                return BigDecimal.ZERO;
            }
            s.setLastCummA(sellAInfo.cummQuoteQty());
            s.setState(DrainSession.State.A_SELL_FILLED);

            // === A BUY возле верхней кромки (ex-self)
            BigDecimal buyPrice = mexcTradeService.getNearUpperSpreadPrice(
                    symbol, chatId, props.getDrain().getDepthLimit());
            s.setPBuy(buyPrice);

            // Планирование количества под MARKET SELL B
            BigDecimal plannedSellQtyB = mexcTradeService.planMarketSellQtyAccountB(symbol, buyPrice, s.getQtyA(), chatId);
            if (plannedSellQtyB.compareTo(BigDecimal.ZERO) <= 0) {
                s.autoPause(DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE, "B не может выставить MARKET SELL ≥ minNotional.");
                return BigDecimal.ZERO;
            }

            BigDecimal spendA = mexcTradeService.reserveForMakerFee(s.getLastCummA());
            BigDecimal capByQty = buyPrice.multiply(plannedSellQtyB);
            if (spendA.compareTo(capByQty) > 0) spendA = capByQty;

            String buyOrderId = mexcTradeService.placeLimitBuyAccountA(symbol, buyPrice, spendA, plannedSellQtyB, chatId);
            s.setBuyOrderId(buyOrderId);
            s.setState(DrainSession.State.A_BUY_PLACED);

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
                s.setState(DrainSession.State.AUTO_PAUSE);
                return BigDecimal.ZERO;
            }
            s.setBuyOrderId(rqBuy.orderId());
            s.setPBuy(rqBuy.price());

            log.info("A ➡ BUY лимитка {} USDT @ {} (maxQty={} ; orderId={})",
                    spendA.stripTrailingZeros(),
                    s.getPBuy().stripTrailingZeros(),
                    plannedSellQtyB.stripTrailingZeros(),
                    s.getBuyOrderId());

            // ПРАВИЛЬНАЯ проверка после BUY
            var vBuyPlaced = reconciler.checkAfterBuyPlaced(symbol, chatId, s);
            if (vBuyPlaced != Reconciler.Verdict.OK) {
                s.autoPause(DrainSession.AutoPauseReason.FRONT_RUN,
                        "После A-BUY наш bid не топ (верификация).");
                return BigDecimal.ZERO;
            }

            // === B MARKET SELL
            mexcTradeService.marketSellFromAccountB(symbol, buyPrice, plannedSellQtyB, chatId);
            s.setState(DrainSession.State.B_MKT_SELL_SENT);

            // Сверка «после B SELL»
            var vSell = reconciler.checkAfterBSell(symbol, chatId, s);
            if (vSell != Reconciler.Verdict.OK) {
                s.autoPause(DrainSession.AutoPauseReason.PARTIAL_MISMATCH, "После MARKET SELL на B осталась не-пыль.");
                return BigDecimal.ZERO;
            }

            // === Ждём FILLED по A-BUY
            var buyAInfo = mexcTradeService.waitUntilFilled(symbol, s.getBuyOrderId(), credsA.getApiKey(), credsA.getSecret(), 6000);
            if (!"FILLED".equals(buyAInfo.status()) || buyAInfo.executedQty().signum() <= 0) {
                s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "A BUY не FILLED (status=" + buyAInfo.status() + ")");
                return BigDecimal.ZERO;
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
            log.error("❌ Ошибка в executeCycleWithGuards", e);
            try {
                BigDecimal tokensA = mexcTradeService.getTokenBalanceAccountA(s.getSymbol(), chatId);
                if (tokensA.compareTo(BigDecimal.ZERO) > 0) {
                    mexcTradeService.forceMarketSellAccountA(s.getSymbol(), tokensA, chatId);
                }
            } catch (Exception ex) {
                log.error("Не удалось аварийно продать остаток A: {}", ex.getMessage());
            }
            s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, e.getClass().getSimpleName());
            return BigDecimal.ZERO;
        }
    }


    // Ручная пауза
    public void requestStop(Long chatId) {
        var s = MemoryDb.getSession(chatId);
        if (s == null) return;
        s.autoPause(DrainSession.AutoPauseReason.MANUAL, "Остановлено пользователем.");
    }

    // Продолжить из факта балансов: A должен иметь базу ≥ minQty, B — пыль
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
            if (aBase.compareTo(f.minQty) < 0) {
                tg.reply(chatId, "❌ На A мало базового токена для продолжения (нужно ≥ minQty).");
                return;
            }
            s.setQtyA(aBase);
            s.setState(DrainSession.State.A_MKT_BUY_DONE);
            MemoryDb.setSession(chatId, s);

            for (int i = 0; i < cycles; i++) {
                s.setCycleIndex(i + 1);
                BigDecimal next = executeCycleWithGuards(chatId, s);
                if (s.getState() == DrainSession.State.AUTO_PAUSE) {
                    tg.reply(chatId, "⏸ Автопауза: %s – %s".formatted(s.getReason(), s.getReasonDetails()));
                    break;
                }
                if (next == null || next.signum() <= 0) break;
                s.setQtyA(next);
            }
        } finally {
            MemoryDb.getFlag(chatId).set(false);
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
