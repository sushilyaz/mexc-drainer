package com.suhoi.mexcdrainer.service;

import com.suhoi.mexcdrainer.util.MemoryDb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Сервис перелива USDT между двумя аккаунтами через монету с большим спредом.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DrainService {

    private final MexcTradeService mexcTradeService;

    /**
     * Запуск перелива
     *
     * @param symbol     тикер (например, ANTUSDT)
     * @param usdtAmount сумма в USDT для перелива
     * @param chatId     id чата Телеграма для обратной связи
     */
    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId, int cycles) {
        try {
            log.info("🚀 Запуск перелива: символ={}, сумма={} USDT", symbol, usdtAmount);

            // ШАГ 1 MARKET BUY A (берём фактическое executedQty)
            MexcTradeService.OrderInfo buyA = mexcTradeService.marketBuyAccountAFull(symbol, usdtAmount, chatId);
            if (buyA == null || buyA.executedQty().signum() <= 0) {
                log.error("A ➡ Market BUY не дал executedQty. Статус={}", buyA == null ? "null" : buyA.status());
                return;
            }
            BigDecimal qtyTokens = buyA.executedQty();
            log.info("A ➡ Купил на {} USDT, получил {} токенов (avg={})",
                    buyA.cummQuoteQty().stripTrailingZeros().toPlainString(),
                    qtyTokens.stripTrailingZeros().toPlainString(),
                    buyA.avgPrice().stripTrailingZeros().toPlainString()
            );

            // 2) Циклы
            for (int i = 0; i < cycles; i++) {
                log.info("🔄 Цикл {}/{}", i + 1, cycles);
                qtyTokens = executeCycle(symbol, qtyTokens, chatId); // вернём фактический nextQty
                if (qtyTokens == null || qtyTokens.compareTo(BigDecimal.ZERO) <= 0) {
                    log.warn("⚠ Следующий объём для цикла <= 0 — останавливаемся");
                    break;
                }
            }

        } catch (Exception e) {
            log.error("❌ Ошибка в startDrain", e);
        }
    }

    /**
     * Один цикл перелива, возвращает фактическое количество токенов A после лимитной покупки.
     *
     * @param symbol     тикер
     * @param qtyTokens  сколько токенов продаём изначально (равно фактическому количеству A из прошлого шага)
     * @param chatId     телеграм чат
     * @return           фактически купленное A в конце цикла (executedQty лимитной покупки)
     */
    private BigDecimal executeCycle(String symbol, BigDecimal qtyTokens, Long chatId) {
        try {
            long tCycle = System.currentTimeMillis();

            //  ШАГ 2 A продаёт лимиткой возле нижней границы
            BigDecimal sellPrice = mexcTradeService.getNearLowerSpreadPrice(symbol);
            log.info("🎯 A SELL лимитка: price={} (нижняя граница)", sellPrice.stripTrailingZeros().toPlainString());

            String sellOrderId = mexcTradeService.placeLimitSellAccountA(symbol, sellPrice, qtyTokens, chatId);
            log.info("A ➡ SELL лимитка {} токенов @ {} (orderId={})",
                    qtyTokens.stripTrailingZeros(), sellPrice.stripTrailingZeros(), sellOrderId);

            //  ШАГ 3  B покупает по рынку (на сумму ~ price*qty c учётом комиссии), чтобы заполнить A
            log.info("🧮 План для B BUY: цена={} * qty={} ≈ {} USDT",
                    sellPrice.stripTrailingZeros(), qtyTokens.stripTrailingZeros(),
                    sellPrice.multiply(qtyTokens).stripTrailingZeros());
            mexcTradeService.marketBuyFromAccountB(symbol, sellPrice, qtyTokens, chatId);
            log.info("B ➡ BUY market ~{} токенов @ {} (на сумму ~с учётом комиссии)",
                    qtyTokens.stripTrailingZeros(), sellPrice.stripTrailingZeros());

            //  ШАГ 4  Дожидаемся FILLED по A-SELL, чтобы взять реальный usdtEarned
            MexcTradeService.OrderInfo sellAInfo = null;
            if (sellOrderId != null) {
                var credsA = MemoryDb.getAccountA(chatId);
                sellAInfo = mexcTradeService.waitUntilFilled(symbol, sellOrderId, credsA.getApiKey(), credsA.getSecret(), 6000);
            }
            if (sellAInfo == null || sellAInfo.executedQty().signum() <= 0) {
                log.warn("A SELL не FILLED или executedQty=0. Статус={}", sellAInfo == null ? "null" : sellAInfo.status());
                return BigDecimal.ZERO;
            }
            BigDecimal usdtEarned = sellAInfo.cummQuoteQty(); // фактически пришло на A
            log.info("📊 A получил от SELL {} USDT (executedQty={} @ avg={})",
                    usdtEarned.stripTrailingZeros(),
                    sellAInfo.executedQty().stripTrailingZeros(),
                    sellAInfo.avgPrice().stripTrailingZeros());

            //  ШАГ 5  A готовит лимитный BUY возле верхней границы,
            //    НО сначала планируем, сколько реально сможет продать B (учёт stepSize/minNotional/остатка/комиссии)
            BigDecimal buyPrice = mexcTradeService.getNearUpperSpreadPrice(symbol);
            log.info("🎯 A BUY лимитка: планируем price={} (верхняя граница)", buyPrice.stripTrailingZeros().toPlainString());

            // планируем финальное количество, которое сможет продать B по рынку
            BigDecimal plannedSellQtyB = mexcTradeService.planMarketSellQtyAccountB(symbol, buyPrice, qtyTokens, chatId);
            if (plannedSellQtyB.compareTo(BigDecimal.ZERO) <= 0) {
                log.warn("B не может выставить MARKET SELL ≥ minNotional. Прерываем цикл.");
                return BigDecimal.ZERO;
            }

            // оставим чуть USDT на возможную maker-комиссию при лимитном BUY A
            BigDecimal spendA = mexcTradeService.reserveForMakerFee(usdtEarned);
            // не покупаем больше, чем B реально сольёт
            BigDecimal capByQty = buyPrice.multiply(plannedSellQtyB);
            if (spendA.compareTo(capByQty) > 0) spendA = capByQty;

            String buyOrderId = mexcTradeService.placeLimitBuyAccountA(symbol, buyPrice, spendA, plannedSellQtyB, chatId);
            log.info("A ➡ BUY лимитка на {} USDT @ {} (maxQty={} ; orderId={})",
                    spendA.stripTrailingZeros(), buyPrice.stripTrailingZeros(),
                    plannedSellQtyB.stripTrailingZeros(), buyOrderId);

            //  ШАГ 6  B продаёт по рынку ровно то количество, которое мы заложили в BUY A
            mexcTradeService.marketSellFromAccountB(symbol, buyPrice, plannedSellQtyB, chatId);
            log.info("B ➡ SELL market {} токенов @ {}", plannedSellQtyB.stripTrailingZeros(), buyPrice.stripTrailingZeros());

            //  ШАГ 7  Дожидаемся FILLED по A-BUY, возвращаем фактическое количество на следующий цикл
            MexcTradeService.OrderInfo buyAInfo = null;
            if (buyOrderId != null) {
                var credsA = MemoryDb.getAccountA(chatId);
                buyAInfo = mexcTradeService.waitUntilFilled(symbol, buyOrderId, credsA.getApiKey(), credsA.getSecret(), 6000);
            }
            if (buyAInfo == null || buyAInfo.executedQty().signum() <= 0) {
                log.warn("A BUY не FILLED или executedQty=0. Статус={}", buyAInfo == null ? "null" : buyAInfo.status());
                return BigDecimal.ZERO;
            }

            long dt = System.currentTimeMillis() - tCycle;
            log.info("✅ Цикл завершён за {} ms. A получил {} токенов (avg={}), потратил {} USDT.",
                    dt,
                    buyAInfo.executedQty().stripTrailingZeros().toPlainString(),
                    buyAInfo.avgPrice().stripTrailingZeros().toPlainString(),
                    buyAInfo.cummQuoteQty().stripTrailingZeros().toPlainString());

            return buyAInfo.executedQty();

        } catch (Exception e) {
            log.error("❌ Ошибка в executeCycle", e);
            try {
                BigDecimal tokensA = mexcTradeService.getTokenBalanceAccountA(symbol, chatId);
                if (tokensA.compareTo(BigDecimal.ZERO) > 0) {
                    mexcTradeService.forceMarketSellAccountA(symbol, tokensA, chatId);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return BigDecimal.ZERO;
        }
    }
}
