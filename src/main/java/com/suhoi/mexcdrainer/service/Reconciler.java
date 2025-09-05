package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.config.AppProperties;
import com.suhoi.mexcdrainer.model.DrainSession;
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

    public enum Verdict { OK, NEED_REQUOTE, AUTO_PAUSE }

    private int ticksBetween(BigDecimal a, BigDecimal b, BigDecimal tick) {
        if (a == null || b == null || tick == null || tick.signum() <= 0) return Integer.MAX_VALUE;
        BigDecimal diff = a.subtract(b).abs();
        return diff.divide(tick, 0, BigDecimal.ROUND_HALF_UP).intValue();
    }

    /** Проверка: после размещения A-SELL наша цена остаётся top ask (ex-self). */
    public Verdict checkAfterSellPlaced(String symbol, Long chatId, DrainSession s) {
        var dl = props.getDrain().getDepthLimit();
        var top = mexc.topExcludingSelf(symbol, chatId, dl);
        var f   = mexc.getSymbolFilters(symbol);
        if (top == null || f == null) return Verdict.AUTO_PAUSE;

        BigDecimal bid = top.bid();
        BigDecimal ask = top.ask();

        int spreadTicks = ticksBetween(ask, bid, f.tickSize);
        if (spreadTicks < props.getDrain().getMinSpreadTicks()) {
            log.warn("Спред сжался: {} тиков (< min {}).", spreadTicks, props.getDrain().getMinSpreadTicks());
            return Verdict.AUTO_PAUSE;
        }

        // Наш SELL должен быть (примерно) top ask
        int off = ticksBetween(ask, s.getPSell(), f.tickSize);
        if (off <= props.getDrain().getEpsilonTicks()) return Verdict.OK;

        // Кто-то вклинился между нами и нижней кромкой — переставляем.
        if (s.getRequotesSell() < props.getDrain().getMaxRequotesPerLeg()) return Verdict.NEED_REQUOTE;
        return Verdict.AUTO_PAUSE;
    }

    /** Проверка после MARKET BUY со стороны B: факт списания USDT и появление base на B. */
    public Verdict checkAfterBBuy(String symbol, Long chatId, DrainSession s) {
        BigDecimal bBase = mexc.getTokenBalanceAccountB(symbol, chatId);
        if (bBase == null) bBase = BigDecimal.ZERO;

        // На B должна появиться база примерно = qtyA (допуски по stepSize).
        var f = mexc.getSymbolFilters(symbol);
        BigDecimal step = f.stepSize;
        BigDecimal dust = step.max(new BigDecimal("0.00000001"));

        // Если «базы» на B почти нет — кто-то другой реализовал наш SELL до того, как B купил.
        if (bBase.compareTo(dust) < 0) {
            return Verdict.AUTO_PAUSE;
        }
        return Verdict.OK;
    }

    /** Проверка после размещения A-BUY: наша цена остаётся top bid (ex-self). */
    public Verdict checkAfterBuyPlaced(String symbol, Long chatId, DrainSession s) {
        var dl = props.getDrain().getDepthLimit();
        var top = mexc.topExcludingSelf(symbol, chatId, dl);
        var f   = mexc.getSymbolFilters(symbol);
        if (top == null || f == null) return Verdict.AUTO_PAUSE;

        BigDecimal bid = top.bid();
        BigDecimal ask = top.ask();
        int spreadTicks = ticksBetween(ask, bid, f.tickSize);
        if (spreadTicks < props.getDrain().getMinSpreadTicks()) {
            log.warn("Спред сжался: {} тиков (< min {}).", spreadTicks, props.getDrain().getMinSpreadTicks());
            return Verdict.AUTO_PAUSE;
        }

        int off = ticksBetween(s.getPBuy(), bid, f.tickSize);
        if (off <= props.getDrain().getEpsilonTicks()) return Verdict.OK;

        if (s.getRequotesBuy() < props.getDrain().getMaxRequotesPerLeg()) return Verdict.NEED_REQUOTE;
        return Verdict.AUTO_PAUSE;
    }

    /** Проверка после MARKET SELL на B: на B должна остаться пыль, а A получить базовый актив. */
    public Verdict checkAfterBSell(String symbol, Long chatId, DrainSession s) {
        BigDecimal bBase = mexc.getTokenBalanceAccountB(symbol, chatId);
        var f = mexc.getSymbolFilters(symbol);
        BigDecimal step = f.stepSize;
        BigDecimal dust = step.max(new BigDecimal("0.00000001"));
        if (bBase.compareTo(dust) <= 0) return Verdict.OK; // пыль допустима
        return Verdict.AUTO_PAUSE;
    }
}

