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

    /** Проверка после MARKET/IOC SELL на B.
     *  Раньше требовали «на B осталась пыль», что неверно, когда A-BUY не покрывает весь объём.
     *  Теперь считаем ожидаемый остаток: qtyA(на входе цикла) - plannedSellQtyB.
     *  Если фактический остаток на B <= ожидаемого + небольшой допуск — всё ок.
     */
    public Verdict checkAfterBSell(String symbol, Long chatId, DrainSession s) {
        BigDecimal bNow = mexc.getTokenBalanceAccountB(symbol, chatId);
        if (bNow == null) bNow = BigDecimal.ZERO;

        var f = mexc.getSymbolFilters(symbol);
        BigDecimal step = (f != null && f.stepSize != null && f.stepSize.signum() > 0)
                ? f.stepSize
                : new BigDecimal("1"); // минимальный безопасный шаг в штуках токена, если вдруг нет фильтров

        // База B перед SELL с учётом хвостов прошлых циклов.
        BigDecimal bBefore = (s.getBBaseBeforeSell() != null) ? s.getBBaseBeforeSell() : BigDecimal.ZERO;
        BigDecimal planned = (s.getPlannedSellQtyB() != null) ? s.getPlannedSellQtyB() : BigDecimal.ZERO;

        // Ожидаемый остаток: что было на B перед продажей минус план продаж.
        BigDecimal expected = bBefore.subtract(planned);
        if (expected.signum() < 0) expected = BigDecimal.ZERO;

        // Допуск — пару-тройку шагов количества.
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

