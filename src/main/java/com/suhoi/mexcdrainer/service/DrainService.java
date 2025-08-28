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

            // 1) MARKET BUY A (берём фактическое executedQty)
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
     * @param symbol    тикер
     * @param qtyTokens сколько токенов продаём изначально (равно фактическому количеству A из прошлого шага)
     * @param chatId    телеграм чат
     * @return фактически купленное A в конце цикла (executedQty лимитной покупки)
     */
    private BigDecimal executeCycle(String symbol, BigDecimal qtyTokens, Long chatId) {
        try {
            long tCycle = System.currentTimeMillis();

            // 1) A продаёт лимиткой возле нижней границы
            BigDecimal sellPrice = mexcTradeService.getNearLowerSpreadPrice(symbol);
            String sellOrderId = mexcTradeService.placeLimitSellAccountA(symbol, sellPrice, qtyTokens, chatId);
            log.info("A ➡ SELL лимитка {} токенов @ {} (orderId={})",
                    qtyTokens.stripTrailingZeros(), sellPrice.stripTrailingZeros(), sellOrderId);

// если SELL не был размещён — нет смысла продолжать цикл
            if (sellOrderId == null) {
                log.warn("A SELL не размещён (minNotional/minQty/входные проверки) — пропускаем цикл");
                return BigDecimal.ZERO;
            }

// перед MARKET BUY B — лог плана по USDT (для сверки)
            BigDecimal plannedQuote = sellPrice.multiply(qtyTokens).setScale(10, RoundingMode.DOWN);
            log.info("🧮 План для B BUY: цена={} * qty={} ≈ {} USDT",
                    sellPrice.stripTrailingZeros().toPlainString(),
                    qtyTokens.stripTrailingZeros().toPlainString(),
                    plannedQuote.stripTrailingZeros().toPlainString());

// 2) B покупает
            mexcTradeService.marketBuyFromAccountB(symbol, sellPrice, qtyTokens, chatId);
            log.info("B ➡ BUY market ~{} токенов @ {} (на сумму ~{})",
                    qtyTokens.stripTrailingZeros(),
                    sellPrice.stripTrailingZeros(),
                    plannedQuote.stripTrailingZeros());


            // 3) Дожидаемся FILLED по A-SELL, чтобы взять реальный usdtEarned
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

            // 4) A выставляет лимитный BUY возле верхней границы на эти деньги
            BigDecimal buyPrice = mexcTradeService.getNearUpperSpreadPrice(symbol);
            String buyOrderId = mexcTradeService.placeLimitBuyAccountA(symbol, buyPrice, usdtEarned, chatId);
            log.info("A ➡ BUY лимитка на {} USDT @ {} (orderId={})",
                    usdtEarned.stripTrailingZeros(), buyPrice.stripTrailingZeros(), buyOrderId);

            // 5) B продаёт по рынку исходное количество qtyTokens
            mexcTradeService.marketSellFromAccountB(symbol, buyPrice, qtyTokens, chatId);
            log.info("B ➡ SELL market {} токенов @ {}", qtyTokens.stripTrailingZeros(), buyPrice.stripTrailingZeros());

            // 6) Дожидаемся FILLED по A-BUY, возвращаем фактическое количество на следующий цикл
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
