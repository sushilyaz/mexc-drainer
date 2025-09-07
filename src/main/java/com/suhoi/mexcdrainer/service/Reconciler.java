package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.DrainSession;
import com.suhoi.mexcdrainer.ws.dto.BookTicker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class Reconciler {

    private final MexcTradeService mexc;        // REST и бизнес-логика
    private final AppProperties props;
    private final MexcTradeWsService ws;        // <-- фасад WS

    public enum Verdict { OK, NEED_REQUOTE, AUTO_PAUSE }

    private int ticksBetween(BigDecimal a, BigDecimal b, BigDecimal tick) {
        if (a == null || b == null || tick == null || tick.signum() <= 0) return Integer.MAX_VALUE;
        BigDecimal diff = a.subtract(b).abs();
        return diff.divide(tick, 0, BigDecimal.ROUND_HALF_UP).intValue();
    }

    private Optional<BookTicker> wsTop(String symbol) {
        try { return ws.wsBest(symbol); } catch (Exception e) { return Optional.empty(); }
    }

    /** После размещения A-SELL наша цена остаётся топ ask (исключая нас самих). */
    public Verdict checkAfterSellPlaced(String symbol, Long chatId, DrainSession s) {
        var f = mexc.getSymbolFilters(symbol);
        if (f == null) return Verdict.AUTO_PAUSE;

        var top = wsTop(symbol);
        if (top.isEmpty()) return Verdict.OK; // в отсутствие данных не стопорим цикл

        BigDecimal bid = top.get().getBidPrice();
        BigDecimal ask = top.get().getAskPrice();

        int spreadTicks = ticksBetween(ask, bid, f.tickSize);
        if (spreadTicks < props.getDrain().getMinSpreadTicks()) {
            log.warn("Спред сжался: {} тиков (< min {}).", spreadTicks, props.getDrain().getMinSpreadTicks());
            return Verdict.AUTO_PAUSE;
        }

        BigDecimal effAsk = ask;
        int offSelf = ticksBetween(ask, s.getPSell(), f.tickSize);
        if (offSelf <= props.getDrain().getEpsilonTicks()) {
            effAsk = ask.add(f.tickSize.multiply(BigDecimal.valueOf(props.getDrain().getEpsilonTicks())));
        }

        int off = ticksBetween(effAsk, s.getPSell(), f.tickSize);
        if (off <= props.getDrain().getEpsilonTicks()) return Verdict.OK;
        if (s.getRequotesSell() < props.getDrain().getMaxRequotesPerLeg()) return Verdict.NEED_REQUOTE;
        return Verdict.AUTO_PAUSE;
    }

    /** После MARKET/IOC BUY со стороны B: проверить, что база на B появилась. */
    public Verdict checkAfterBBuy(String symbol, Long chatId, DrainSession s) {
        BigDecimal bBase = mexc.getTokenBalanceAccountB(symbol, chatId);
        if (bBase == null) bBase = BigDecimal.ZERO;

        var f = mexc.getSymbolFilters(symbol);
        BigDecimal step = f.stepSize;
        BigDecimal dust = step.max(new BigDecimal("0.00000001"));

        if (bBase.compareTo(dust) < 0) return Verdict.AUTO_PAUSE;
        return Verdict.OK;
    }

    /** После размещения A-BUY: наша цена остаётся top bid (исключая нас самих). */
    public Verdict checkAfterBuyPlaced(String symbol, Long chatId, DrainSession s) {
        var f = mexc.getSymbolFilters(symbol);
        if (f == null) return Verdict.AUTO_PAUSE;

        var top = wsTop(symbol);
        if (top.isEmpty()) return Verdict.OK;

        BigDecimal bid = top.get().getBidPrice();
        BigDecimal ask = top.get().getAskPrice();

        int spreadTicks = ticksBetween(ask, bid, f.tickSize);
        if (spreadTicks < props.getDrain().getMinSpreadTicks()) {
            log.warn("Спред сжался: {} тиков (< min {}).", spreadTicks, props.getDrain().getMinSpreadTicks());
            return Verdict.AUTO_PAUSE;
        }

        BigDecimal effBid = bid;
        int offSelf = ticksBetween(s.getPBuy(), bid, f.tickSize);
        if (offSelf <= props.getDrain().getEpsilonTicks()) {
            effBid = bid.subtract(f.tickSize.multiply(BigDecimal.valueOf(props.getDrain().getEpsilonTicks())));
        }

        int off = ticksBetween(s.getPBuy(), effBid, f.tickSize);
        if (off <= props.getDrain().getEpsilonTicks()) return Verdict.OK;

        if (s.getRequotesBuy() < props.getDrain().getMaxRequotesPerLeg()) return Verdict.NEED_REQUOTE;
        return Verdict.AUTO_PAUSE;
    }

    /** После SELL на B: допускаем ожидаемый остаток (как у тебя уже реализовано). */
    public Verdict checkAfterBSell(String symbol, Long chatId, DrainSession s) {
        BigDecimal bNow = mexc.getTokenBalanceAccountB(symbol, chatId);
        if (bNow == null) bNow = BigDecimal.ZERO;

        var f = mexc.getSymbolFilters(symbol);
        BigDecimal step = (f != null && f.stepSize != null && f.stepSize.signum() > 0) ? f.stepSize : new BigDecimal("1");

        BigDecimal bBefore = (s.getBBaseBeforeSell() != null) ? s.getBBaseBeforeSell() : BigDecimal.ZERO;
        BigDecimal planned = (s.getPlannedSellQtyB() != null) ? s.getPlannedSellQtyB() : BigDecimal.ZERO;

        BigDecimal expected = bBefore.subtract(planned);
        if (expected.signum() < 0) expected = BigDecimal.ZERO;

        BigDecimal tolerance = step.multiply(new BigDecimal("3"));
        BigDecimal limit = expected.add(tolerance).max(step);

        if (bNow.compareTo(limit) <= 0) return Verdict.OK;

        log.warn("После B-SELL остаток на B больше ожидаемого: actual={} > limit(=expected {} + tol {}) [bBefore={}, plannedSellQtyB={}]",
                bNow.stripTrailingZeros().toPlainString(),
                expected.stripTrailingZeros().toPlainString(),
                tolerance.stripTrailingZeros().toPlainString(),
                bBefore.stripTrailingZeros().toPlainString(),
                planned.stripTrailingZeros().toPlainString());
        return Verdict.AUTO_PAUSE;
    }
}
