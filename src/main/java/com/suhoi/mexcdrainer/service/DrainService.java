package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.config.WsUserProperties;
import com.suhoi.mexcdrainer.model.DrainSession;
import com.suhoi.mexcdrainer.util.MemoryDb;
import com.suhoi.mexcdrainer.ws.facade.BalanceWsFacade;
import com.suhoi.mexcdrainer.ws.facade.MarketBookFacade;
import com.suhoi.mexcdrainer.ws.facade.UserOrderAwaiter;
import com.suhoi.mexcdrainer.ws.facade.WsEnsureFacade;
import com.suhoi.mexcdrainer.ws.user.OwnOrdersRegistry;
import com.suhoi.mexcdrainer.ws.user.UserWsManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Service
@Slf4j
@RequiredArgsConstructor
public class DrainService {

    private static final boolean FAST_CROSS_IOC = true;
    private static final int FAST_ENSURE_GRACE_MS = 150;
    private static final int FAST_MAX_REQUOTES = 1;
    private static final int BOOK_GLUE_SLEEP_MS = 15;
    private static final BigDecimal SPREAD_GUARD = new BigDecimal("0.5"); // середина спреда

    private final MexcTradeService mexc;
    private final Reconciler reconciler;
    private final AppProperties props;
    private final TelegramService tg;

    // WS-инфраструктура/фасады
    private final UserWsManager userWsManager;
    private final WsUserProperties wsUserProps;
    private final MarketBookFacade book;
    private final UserOrderAwaiter awaiter;
    private final BalanceWsFacade balanceWs;
    private final WsEnsureFacade wsEnsure;

    /* ===== helpers ===== */

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

    private static void sleep(int ms) { try { Thread.sleep(ms); } catch (InterruptedException ignored) {} }

    /* ===== entrypoints ===== */

    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId, int cycles) {
        var flag = MemoryDb.getFlag(chatId);
        if (!flag.compareAndSet(false, true)) {
            tg.reply(chatId, "⏳ Уже идёт перелив в этом чате.");
            return;
        }
        try {
            // гарантируем приватные WS по A и B (чтобы ждать статусы без REST)
            var credsA = MemoryDb.getAccountA(chatId);
            var credsB = MemoryDb.getAccountB(chatId);
            userWsManager.ensure(credsA.getApiKey(), credsA.getSecret());
            userWsManager.ensure(credsB.getApiKey(), credsB.getSecret());

            log.info("🚀 START_DRAIN: symbol={}, amount={} USDT, cycles={}", symbol, fmt(usdtAmount), cycles);

            // 0) Рынок BUY на A
            var buyA = mexc.marketBuyAccountAFull(symbol, usdtAmount, chatId);
            log.info("A_MKT_BUY_RESULT: status={} exec={} cQuote={} avg={}",
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

            tg.reply(chatId, "✅ A купил ~%s токенов @avg=%s".formatted(
                    s.getQtyA().stripTrailingZeros(), buyA.avgPrice().stripTrailingZeros()));

            int i = 0; boolean unlimited = (cycles <= 0);
            while (unlimited || i < cycles) {
                s.setCycleIndex(++i);
                log.info("===== CYCLE_START #{} {}", s.getCycleIndex(), snapshot(s));
                BigDecimal next = executeCycleWsFirst(chatId, s);
                log.info("===== CYCLE_END   #{} -> nextQtyA={} {}", s.getCycleIndex(), fmt(next), snapshot(s));

                if (s.getState() == DrainSession.State.AUTO_PAUSE) {
                    tg.reply(chatId, "⏸ Автопауза: %s – %s".formatted(s.getReason(), s.getReasonDetails()));
                    break;
                }
                if (next == null || next.signum() <= 0) break;
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

    private BigDecimal executeCycleWsFirst(Long chatId, DrainSession s) {
        final String symbol = s.getSymbol();
        var cfg = props.getDrain();
        var f   = mexc.getSymbolFilters(symbol);

        try {
            /* === (1) SELL на A: цена от WS со self-exclusion на первом уровне === */
            var credsA = MemoryDb.getAccountA(chatId);
            BigDecimal nearSell = book.nearLowerSpread(symbol, credsA.getApiKey(), SPREAD_GUARD, f.getTickSize())
                    .orElse(f.getTickSize().signum() > 0 ? f.getTickSize() : new BigDecimal("0.00000001"));
            BigDecimal minQtyForSell = minQtyForNotional(nearSell, f);
            if (s.getQtyA() == null || s.getQtyA().compareTo(minQtyForSell) < 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "qtyA < minNotional для SELL @ " + fmt(nearSell) + " (qtyA=" + fmt(s.getQtyA()) + ", min=" + fmt(minQtyForSell) + ")",
                        "PRE-A-SELL-MIN");
            }

            var placedSell = mexc.placeLimitSellAccountAPlaced(symbol, nearSell, s.getQtyA(), chatId);
            s.setSellOrderId(placedSell.orderId());
            s.setPSell(placedSell.price());
            s.setQtyA(placedSell.qty());
            s.setState(DrainSession.State.A_SELL_PLACED);
            userWsManager.registerPlaced(
                    credsA.getApiKey(),
                    symbol,
                    placedSell.orderId(),          // exch order id (главный)
                    placedSell.orderId(),          // если есть отдельный clientOrderId — подставь его сюда
                    OwnOrdersRegistry.Side.SELL,
                    placedSell.price(),
                    placedSell.qty()
            );

            /* === (1a) B IOC BUY (быстрый путь). Статус B пока проверяем старым способом (Reconciler),
                   т.к. методы B у тебя возвращают без orderId. Когда начнёшь возвращать ID — заменим на awaiter. */
            if (FAST_CROSS_IOC) {
                sleep(BOOK_GLUE_SLEEP_MS);
                mexc.limitIocBuyFromAccountB(symbol, s.getPSell(), s.getQtyA(), chatId);
                s.setLastSpentB(s.getPSell().multiply(s.getQtyA()));
                s.setState(DrainSession.State.B_MKT_BUY_SENT);

                var vBuyBFast = reconciler.checkAfterBBuy(symbol, chatId, s);
                if (vBuyBFast != Reconciler.Verdict.OK) {
                    var rqSell = wsEnsure.ensureTopAskOrRequoteSellWs(
                            symbol, chatId,
                            s.getSellOrderId(), s.getPSell(), s.getQtyA(),
                            Math.min(FAST_MAX_REQUOTES, cfg.getMaxRequotesPerLeg()),
                            cfg.getEpsilonTicks(),
                            Math.min(FAST_ENSURE_GRACE_MS, cfg.getPostPlaceGraceMs())
                    );
                    if (!rqSell.ok()) {
                        return autoPauseAndZero(s, DrainSession.AutoPauseReason.UNKNOWN,
                                "ensureTopAskOrRequoteSellWs (fast) -> not ok", "A-SELL-ENSURE-FAST");
                    }
                    s.setSellOrderId(rqSell.orderId());
                    s.setPSell(rqSell.price());
                    userWsManager.registerPlaced(
                            credsA.getApiKey(),
                            symbol,
                            rqSell.orderId(),              // новый exch order id после реквоута
                            rqSell.orderId(),              // если нет отдельного clientId — дублируем
                            OwnOrdersRegistry.Side.SELL,
                            rqSell.price(),
                            s.getQtyA()
                    );
                    BigDecimal spent = mexc.marketBuyFromAccountB(symbol, s.getPSell(), s.getQtyA(), chatId, true);
                    s.setLastSpentB(spent);
                    s.setState(DrainSession.State.B_MKT_BUY_SENT);

                    var vBuyBFb = reconciler.checkAfterBBuy(symbol, chatId, s);
                    if (vBuyBFb != Reconciler.Verdict.OK) {
                        return autoPauseAndZero(s, DrainSession.AutoPauseReason.PARTIAL_MISMATCH,
                                "После MARKET BUY на B база отсутствует.", "B-BUY-VERIFY");
                    }
                }
            }

            /* === (2) Ждём FILLED по A-SELL — ТОЛЬКО WS === */
            var sellA = awaiter.awaitFinal(credsA.getApiKey(), s.getSellOrderId());
            if (!"FILLED".equalsIgnoreCase(sellA.getStatus()) || sellA.getExecutedQty().signum() <= 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.TIMEOUT,
                        "A SELL не FILLED (status=" + sellA.getStatus() + ")", "A-SELL-WAIT");
            }
            s.setLastCummA(sellA.getCummQuoteQty());
            s.setQtyA(sellA.getExecutedQty());
            s.setState(DrainSession.State.A_SELL_FILLED);

            /* === (3) BUY на A возле верхней кромки (WS) === */
            BigDecimal nearBuy = book.nearUpperSpread(symbol, credsA.getApiKey(), SPREAD_GUARD, f.getTickSize())
                    .orElse(f.getTickSize().signum() > 0 ? f.getTickSize() : new BigDecimal("0.00000001"));

            BigDecimal plannedSellQtyB = mexc.planMarketSellQtyAccountB(symbol, nearBuy, s.getQtyA(), chatId);
            if (plannedSellQtyB.compareTo(BigDecimal.ZERO) <= 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                        "B не может выставить MARKET SELL ≥ minNotional.", "B-SELL-PLAN");
            }

            BigDecimal spendA = mexc.reserveForMakerFee(s.getLastCummA());
            BigDecimal capByQty = nearBuy.multiply(plannedSellQtyB);
            if (spendA.compareTo(capByQty) > 0) spendA = capByQty;

            var placedBuy = mexc.placeLimitBuyAccountAPlaced(symbol, nearBuy, spendA, plannedSellQtyB, chatId);
            s.setBuyOrderId(placedBuy.orderId());
            s.setPBuy(placedBuy.price());
            plannedSellQtyB = placedBuy.qty();
            s.setPlannedSellQtyB(plannedSellQtyB);
            userWsManager.registerPlaced(
                    credsA.getApiKey(),
                    symbol,
                    placedBuy.orderId(),
                    placedBuy.orderId(),
                    com.suhoi.mexcdrainer.ws.user.OwnOrdersRegistry.Side.BUY,
                    placedBuy.price(),
                    placedBuy.qty()
            );
            /* === (3a) B SELL IOC + fallback-проверка старым способом (пока без ID B-ордера) === */
            if (FAST_CROSS_IOC) {
                sleep(BOOK_GLUE_SLEEP_MS);
                // снимок базы на B — нужен твоему Reconciler
                s.setBBaseBeforeSell(mexc.getTokenBalanceAccountB(symbol, chatId));
                mexc.limitSellBelowSpreadAccountB(symbol, plannedSellQtyB, chatId);
                s.setState(DrainSession.State.B_MKT_SELL_SENT);

                var vSellB = reconciler.checkAfterBSell(symbol, chatId, s);
                if (vSellB != Reconciler.Verdict.OK) {
                    try {
                        mexc.cancelOrderAccountA(symbol, s.getBuyOrderId(), chatId);
                    } catch (Exception ignore) { }
                    return autoPauseAndZero(s, DrainSession.AutoPauseReason.PARTIAL_MISMATCH,
                            "После B SELL остаток больше ожидаемого.", "B-SELL-VERIFY");
                }
            }

            /* === (4) Ждём FILLED по A-BUY — ТОЛЬКО WS === */
            var buyA = awaiter.awaitFinal(credsA.getApiKey(), s.getBuyOrderId());
            if (!"FILLED".equalsIgnoreCase(buyA.getStatus()) || buyA.getExecutedQty().signum() <= 0) {
                return autoPauseAndZero(s, DrainSession.AutoPauseReason.TIMEOUT,
                        "A BUY не FILLED (status=" + buyA.getStatus() + ")", "A-BUY-WAIT");
            }

            s.setState(DrainSession.State.A_BUY_FILLED);
            return buyA.getExecutedQty();

        } catch (Exception e) {
            log.error("❌ Ошибка в executeCycleWsFirst: {} | {}", e.getClass().getSimpleName(), e.getMessage(), e);
            log.error("SESSION_ON_EXCEPTION {}", snapshot(s));
            // Аварийный свип A (как у тебя)
            try {
                BigDecimal tokensA = mexc.getTokenBalanceAccountA(s.getSymbol(), chatId);
                if (tokensA.compareTo(BigDecimal.ZERO) > 0) {
                    mexc.forceMarketSellAccountA(s.getSymbol(), tokensA, chatId);
                }
            } catch (Exception ex) {
                log.error("Не удалось аварийно продать остаток A: {}", ex.getMessage(), ex);
            }
            s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, e.getClass().getSimpleName());
            return BigDecimal.ZERO;
        }
    }

    // как у тебя, но оставляю, т.к. она про фильтры, а не про источник книги
    private BigDecimal minQtyForNotional(BigDecimal price, MexcTradeService.SymbolFilters f) {
        if (f == null) return BigDecimal.ZERO;
        BigDecimal minQty = (f.getMinQty() != null) ? f.getMinQty() : BigDecimal.ZERO;
        if (f.getMinNotional() == null || f.getMinNotional().signum() <= 0 || price == null || price.signum() <= 0) {
            return minQty;
        }
        BigDecimal raw = f.getMinNotional().divide(price, 16, RoundingMode.UP);
        if (f.getStepSize() != null && f.getStepSize().signum() > 0) {
            BigDecimal steps = raw.divide(f.getStepSize(), 0, RoundingMode.UP);
            return steps.multiply(f.getStepSize()).max(minQty);
        }
        return raw.max(minQty);
    }

    private void finalSweepSellIfPossible(Long chatId, String symbol) {
        try {
            var f = mexc.getSymbolFilters(symbol);
            // сначала пробуем WS-баланс; если устарел — REST
            var credsA = MemoryDb.getAccountA(chatId);
            BigDecimal qtyA = balanceWs.freshAvailable(credsA.getApiKey(), symbol.replace("USDT", ""))
                    .orElseGet(() -> mexc.getTokenBalanceAccountA(symbol, chatId));
            if (qtyA == null || qtyA.signum() <= 0) {
                log.info("FINAL_SWEEP_SKIP: пусто на A");
                return;
            }
            BigDecimal nearSell = book.nearLowerSpread(symbol, credsA.getApiKey(), SPREAD_GUARD, f.getTickSize())
                    .orElse(f.getTickSize().signum() > 0 ? f.getTickSize() : new BigDecimal("0.00000001"));
            BigDecimal minQtyForSell = minQtyForNotional(nearSell, f);

            if (qtyA.compareTo(minQtyForSell) >= 0) {
                mexc.forceMarketSellAccountA(symbol, qtyA, chatId);
                tg.reply(chatId, "🧹 Финальный SELL: продал остаток %s %s".formatted(
                        qtyA.stripTrailingZeros().toPlainString(), symbol.replace("USDT", "")));
            } else {
                log.info("FINAL_SWEEP_SKIP: qtyA={} < minQtyForSell={} (пыль).", fmt(qtyA), fmt(minQtyForSell));
            }
        } catch (Exception e) {
            log.error("FINAL_SWEEP_ERROR: {}", e.getMessage(), e);
        }
    }

    // Ручная пауза
    public void requestStop(Long chatId) {
        var s = MemoryDb.getSession(chatId);
        if (s == null) return;
        s.autoPause(DrainSession.AutoPauseReason.MANUAL, "Остановлено пользователем.");
        log.warn("🛑 MANUAL_STOP {}", snapshot(s));
    }

    // Продолжение из факта балансов — пробуем WS сначала
    public void continueFromBalances(String symbol, Long chatId, int cycles) {
        var flag = MemoryDb.getFlag(chatId);
        if (!flag.compareAndSet(false, true)) {
            tg.reply(chatId, "⏳ Уже идёт перелив в этом чате.");
            return;
        }
        try {
            var s = new DrainSession();
            s.setSymbol(symbol);

            var f = mexc.getSymbolFilters(symbol);

            var credsA = MemoryDb.getAccountA(chatId);
            BigDecimal aBase = balanceWs.freshAvailable(credsA.getApiKey(), symbol.replace("USDT", ""))
                    .orElseGet(() -> mexc.getTokenBalanceAccountA(symbol, chatId));

            log.info("CONTINUE_FROM_BALANCES: aBase={}, minQty={}", fmt(aBase), fmt(f.getMinQty()));
            if (aBase.compareTo(f.getMinQty()) < 0) {
                tg.reply(chatId, "❌ На A мало базового токена для продолжения (нужно ≥ minQty).");
                log.warn("CONTINUE_ABORT: insufficient A base.");
                return;
            }
            s.setQtyA(aBase);
            s.setState(DrainSession.State.A_MKT_BUY_DONE);
            MemoryDb.setSession(chatId, s);

            int i = 0; boolean unlimited = (cycles <= 0);
            while (unlimited || i < cycles) {
                s.setCycleIndex(++i);
                BigDecimal next = executeCycleWsFirst(chatId, s);
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
