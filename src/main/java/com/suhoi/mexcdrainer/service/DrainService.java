package com.suhoi.mexcdrainer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

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
    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId) {
        log.info("🚀 Запуск перелива: символ={}, сумма={} USDT", symbol, usdtAmount);

        try {
            // Проверка средств
            if (!mexcTradeService.checkBalances(usdtAmount, chatId)) {
                log.warn("❌ Недостаточно средств на аккаунтах для перелива {} USDT", usdtAmount);
                return;
            }

            // Первый шаг — покупка по рынку
            String orderIdA = mexcTradeService.marketBuyAccountA(symbol, usdtAmount, chatId);
            log.info("✅ Аккаунт A купил по рынку {} на {} USDT, ордер={}", symbol, usdtAmount, orderIdA);

            // Получаем реальное количество токенов на A (уже с учётом комиссии)
            BigDecimal tokensA = mexcTradeService.getTokenBalanceAccountA(symbol, chatId);
            log.info("📊 Баланс токенов на A после покупки: {}", tokensA);

            int cycle = 1;
            while (tokensA.compareTo(BigDecimal.ZERO) > 0) {
                log.info("🔄 Запуск цикла {} ({} токенов)", cycle, tokensA);
                executeCycle(symbol, tokensA, chatId);

                // Обновляем баланс после цикла
                tokensA = mexcTradeService.getTokenBalanceAccountA(symbol, chatId);
                log.info("📊 Новый баланс токенов на A: {}", tokensA);

                cycle++;
            }

            BigDecimal finalUsdt = mexcTradeService.getUsdtBalanceAccountA(chatId);
            log.info("✅ Перелив завершён. Итоговый баланс на A ≈ {} USDT", finalUsdt);

        } catch (Exception e) {
            log.error("❌ Ошибка при выполнении перелива", e);
        }
    }

    /**
     * Один цикл перелива (с реальными балансами)
     *
     * @param symbol   тикер
     * @param qty      количество токенов на аккаунте A
     * @param chatId   id чата
     */
    private void executeCycle(String symbol, BigDecimal qty, Long chatId) {
        try {
            // Лимитка на продажу (A)
            BigDecimal sellPrice = mexcTradeService.getNearLowerSpreadPrice(symbol);
            String limitSellOrderId = mexcTradeService.placeLimitSellAccountA(symbol, sellPrice, qty, chatId);
            log.info("A ➡ Продажа лимиткой: {} @ {}, ордер={}", qty, sellPrice, limitSellOrderId);

            // B выкупает
            mexcTradeService.buyFromAccountB(symbol, sellPrice, qty, chatId);
            log.info("B ➡ Купил {} @ {}", qty, sellPrice);

            // Теперь у A USDT, проверяем баланс
            BigDecimal usdtA = mexcTradeService.getUsdtBalanceAccountA(chatId);
            log.info("📊 Баланс USDT на A после продажи: {}", usdtA);

            if (usdtA.compareTo(BigDecimal.ONE) < 0) {
                log.warn("⚠ Недостаточно USDT для продолжения цикла ({}), выходим", usdtA);
                return;
            }

            // Лимитка на покупку (A)
            BigDecimal buyPrice = mexcTradeService.getNearUpperSpreadPrice(symbol);
            String limitBuyOrderId = mexcTradeService.placeLimitBuyAccountA(symbol, buyPrice, usdtA, chatId);
            log.info("A ➡ Выставил лимитку на покупку на {} USDT @ {}, ордер={}", usdtA, buyPrice, limitBuyOrderId);

            // B продает
            mexcTradeService.sellFromAccountB(symbol, buyPrice, usdtA, chatId);
            log.info("B ➡ Продал в лимитку A {} @ {}", usdtA, buyPrice);

        } catch (Exception e) {
            log.error("❌ Ошибка в цикле перелива", e);
            try {
                // Фейл — чистим позицию
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
