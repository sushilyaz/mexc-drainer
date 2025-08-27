package com.suhoi.mexcdrainer.service;

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

            // 1️⃣ На A покупаем токены на usdtAmount
            mexcTradeService.marketBuyAccountA(symbol, usdtAmount, chatId);
            // Получаем сколько реально купили токенов
            BigDecimal qtyTokens = mexcTradeService.getTokenBalanceAccountA(symbol, chatId);
            log.info("A ➡ Купил на {} USDT, получил {} токенов", usdtAmount, qtyTokens);

            // 2️⃣ Крутим цикл несколько раз
            for (int i = 0; i < cycles; i++) {
                log.info("🔄 Цикл {}/{}", i + 1, cycles);
                executeCycle(symbol, qtyTokens, chatId);

                // можно уточнять qtyTokens каждый раз с биржи, чтобы не накапливалась погрешность
                qtyTokens = mexcTradeService.getTokenBalanceAccountB(symbol, chatId);
                if (qtyTokens.compareTo(BigDecimal.ZERO) <= 0) {
                    log.warn("⚠ У B закончились токены, прерываем перелив");
                    break;
                }
            }

        } catch (Exception e) {
            log.error("❌ Ошибка в startDrain", e);
        }
    }

    /**
     * Один цикл перелива (с реальными балансами)
     *
     * @param symbol   тикер
     * @param chatId   id чата
     */
    private void executeCycle(String symbol, BigDecimal qtyTokens, Long chatId) {
        try {
            // 1️⃣ A продает лимиткой
            BigDecimal sellPrice = mexcTradeService.getNearLowerSpreadPrice(symbol);
            mexcTradeService.placeLimitSellAccountA(symbol, sellPrice, qtyTokens, chatId);
            log.info("A ➡ Выставил лимитный ордер на продажу на сумму {} токенов по цене {}", qtyTokens, sellPrice);

            // 2️⃣ B покупает ровно столько же
            mexcTradeService.marketBuyFromAccountB(symbol, sellPrice, qtyTokens, chatId);
            log.info("B ➡ Купил по рынку {} токенов @ {}", qtyTokens, sellPrice);

            // 3️⃣ Считаем сколько USDT имеет A
            BigDecimal usdtEarned = sellPrice.multiply(qtyTokens).setScale(5, RoundingMode.DOWN);
            log.info("📊 A осталось {} USDT", usdtEarned);

            // 4️⃣ A ставит лимитку на покупку на эту сумму
            BigDecimal buyPrice = mexcTradeService.getNearUpperSpreadPrice(symbol);
            mexcTradeService.placeLimitBuyAccountA(symbol, buyPrice, usdtEarned, chatId);
            log.info("A ➡ Лимитная покупка на {} USDT @ {}", usdtEarned, buyPrice);

            // 5️⃣ B продаёт обратно ровно qtyTokens
            mexcTradeService.marketSellFromAccountB(symbol, buyPrice, qtyTokens, chatId);
            log.info("B ➡ Продал обратно {} токенов @ {}", qtyTokens, buyPrice);

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
        }
    }
}
