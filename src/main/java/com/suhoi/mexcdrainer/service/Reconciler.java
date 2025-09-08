package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.DrainSession;
import com.suhoi.mexcdrainer.ws.market.MarketWsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
@RequiredArgsConstructor
@Slf4j
public class Reconciler {

    private final MexcTradeService mexc;
    private final AppProperties props;
    private final MarketWsService marketWs; // НОВОЕ: быстрый топ из WS

    public enum Verdict { OK, NEED_REQUOTE, AUTO_PAUSE }

    private int ticksBetween(BigDecimal a, BigDecimal b, BigDecimal tick) {
        if (a == null || b == null || tick == null || tick.signum() <= 0) return Integer.MAX_VALUE;
        BigDecimal diff = a.subtract(b).abs();
        // BigDecimal.ROUND_HALF_UP — legacy, но оставим как было
        return diff.divide(tick, 0, BigDecimal.ROUND_HALF_UP).intValue();
    }

    private static record Top(BigDecimal bid, BigDecimal ask) {}

    /** Быстрый топ: сперва WS, если нет — старый mexc.topExcludingSelf(...) */
    private Top fastTop(String symbol, Long chatId, int dl) {
        var t = marketWs.getTop(symbol);
        if (t != null && t.getBid() != null && t.getAsk() != null) {
            return new Top(t.getBid(), t.getAsk());
        }
        var ex = mexc.topExcludingSelf(symbol, chatId, dl); // как раньше (REST/что там внутри)
        return new Top(ex.bid(), ex.ask());
    }

    /** Проверка: после размещения A-SELL наша цена остаётся top ask (ex-self). */
    public Verdict checkAfterSellPlaced(String symbol, Long chatId, DrainSession s) {
        var dl = props.getDrain().getDepthLimit();
        var f  = mexc.getSymbolFilters(symbol);
        if (f == null) return Verdict.AUTO_PAUSE;

        Top top = fastTop(symbol, chatId, dl);
        if (top == null || top.ask() == null || top.bid() == null) return Verdict.AUTO_PAUSE;

        int spreadTicks = ticksBetween(top.ask(), top.bid(), f.tickSize);
        if (spreadTicks < props.getDrain().getMinSpreadTicks()) {
            log.warn("Спред сжался: {} тиков (< min {}).", spreadTicks, props.getDrain().getMinSpreadTicks());
            return Verdict.AUTO_PAUSE;
        }

        // Наш SELL должен быть (примерно) top ask
        int off = ticksBetween(top.ask(), s.getPSell(), f.tickSize);
        if (off <= props.getDrain().getEpsilonTicks()) return Verdict.OK;

        if (s.getRequotesSell() < props.getDrain().getMaxRequotesPerLeg()) return Verdict.NEED_REQUOTE;
        return Verdict.AUTO_PAUSE;
    }

    /** Проверка после MARKET BUY со стороны B: факт списания USDT и появление base на B. */
    public Verdict checkAfterBBuy(String symbol, Long chatId, DrainSession s) {
        BigDecimal bBase = mexc.getTokenBalanceAccountB(symbol, chatId);
        if (bBase == null) bBase = BigDecimal.ZERO;

        var f = mexc.getSymbolFilters(symbol);
        BigDecimal step = f.stepSize;
        BigDecimal dust = step.max(new BigDecimal("0.00000001"));

        if (bBase.compareTo(dust) < 0) {
            return Verdict.AUTO_PAUSE;
        }
        return Verdict.OK;
    }

    /** Проверка после размещения A-BUY: наша цена остаётся top bid (ex-self). */
    public Verdict checkAfterBuyPlaced(String symbol, Long chatId, DrainSession s) {
        var dl = props.getDrain().getDepthLimit();
        var f  = mexc.getSymbolFilters(symbol);
        if (f == null) return Verdict.AUTO_PAUSE;

        Top top = fastTop(symbol, chatId, dl);
        if (top == null || top.ask() == null || top.bid() == null) return Verdict.AUTO_PAUSE;

        int spreadTicks = ticksBetween(top.ask(), top.bid(), f.tickSize);
        if (spreadTicks < props.getDrain().getMinSpreadTicks()) {
            log.warn("Спред сжался: {} тиков (< min {}).", spreadTicks, props.getDrain().getMinSpreadTicks());
            return Verdict.AUTO_PAUSE;
        }

        int off = ticksBetween(s.getPBuy(), top.bid(), f.tickSize);
        if (off <= props.getDrain().getEpsilonTicks()) return Verdict.OK;

        if (s.getRequotesBuy() < props.getDrain().getMaxRequotesPerLeg()) return Verdict.NEED_REQUOTE;
        return Verdict.AUTO_PAUSE;
    }

    /**
     * Проверка после MARKET/IOC SELL на B.
     * Теперь считаем ожидаемый остаток на B: bBefore - plannedSellQtyB, допускаем небольшой шаг.
     */
    public Verdict checkAfterBSell(String symbol, Long chatId, DrainSession s) {
        BigDecimal bNow = mexc.getTokenBalanceAccountB(symbol, chatId);
        if (bNow == null) bNow = BigDecimal.ZERO;

        var f = mexc.getSymbolFilters(symbol);
        BigDecimal step = (f != null && f.stepSize != null && f.stepSize.signum() > 0)
                ? f.stepSize
                : new BigDecimal("1");

        BigDecimal bBefore = (s.getBBaseBeforeSell() != null) ? s.getBBaseBeforeSell() : BigDecimal.ZERO;
        BigDecimal planned = (s.getPlannedSellQtyB() != null) ? s.getPlannedSellQtyB() : BigDecimal.ZERO;

        BigDecimal expected = bBefore.subtract(planned);
        if (expected.signum() < 0) expected = BigDecimal.ZERO;

        BigDecimal tolerance = step.multiply(new BigDecimal("3"));
        BigDecimal limit = expected.add(tolerance).max(step);

        if (bNow.compareTo(limit) <= 0) {
            return Verdict.OK;
        }

        log.warn("После B-SELL остаток на B больше ожидаемого: actual={} > limit(=expected {} + tol {}) [bBefore={}, plannedSellQtyB={}]",
                bNow.stripTrailingZeros().toPlainString(),
                expected.stripTrailingZeros().toPlainString(),
                tolerance.stripTrailingZeros().toPlainString(),
                bBefore.stripTrailingZeros().toPlainString(),
                planned.stripTrailingZeros().toPlainString());
        return Verdict.AUTO_PAUSE;
    }
}
