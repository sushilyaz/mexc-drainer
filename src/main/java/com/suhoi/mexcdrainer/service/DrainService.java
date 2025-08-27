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
     * @param symbol    тикер (например, ANTUSDT)
     * @param usdtAmount сумма в USDT для перелива
     * @param chatId    id чата Телеграма для обратной связи
     */
    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId) {
        log.info("🚀 Запуск перелива: символ={}, сумма={} USDT", symbol, usdtAmount);

        try {
            // Проверка средств
            if (!mexcTradeService.checkBalances(usdtAmount, chatId)) {
                log.warn("❌ Недостаточно средств на аккаунтах для перелива {} USDT", usdtAmount);
                return;
            }

            // Первый цикл включает покупку по рынку
            executeCycle(symbol, usdtAmount, true, chatId);

            // Остаток после первого цикла (примерная потеря ~5-10%)
            BigDecimal remaining = usdtAmount.multiply(BigDecimal.valueOf(0.9));

            int cycle = 2;
            while (remaining.compareTo(BigDecimal.valueOf(1)) > 0) { // пока есть хотя бы $1
                log.info("🔄 Запуск цикла {} с суммой {} USDT", cycle, remaining);
                executeCycle(symbol, remaining, false,  chatId);
                remaining = remaining.multiply(BigDecimal.valueOf(0.9));
                cycle++;
            }

            log.info("✅ Перелив завершён, итоговая сумма ≈ {} USDT", remaining);

        } catch (Exception e) {
            log.error("❌ Ошибка при выполнении перелива", e);
        }
    }

    /**
     * Один цикл перелива
     *
     * @param symbol    тикер
     * @param amount    сумма
     * @param withMarketBuy нужно ли делать покупку по рынку (только для 1 цикла)
     */
    private void executeCycle(String symbol, BigDecimal amount, boolean withMarketBuy, Long chatId) {
        try {
            if (withMarketBuy) {
                String orderIdA = mexcTradeService.marketBuyAccountA(symbol, amount, chatId);
                log.info("Цикл: Аккаунт A купил по рынку {} на {} USDT, ордер={}", symbol, amount, orderIdA);
            }

            // Выставляем лимитку на продажу (Аккаунт A)
            BigDecimal sellPrice = mexcTradeService.getNearLowerSpreadPrice(symbol);
            String limitSellOrderId = mexcTradeService.placeLimitSellAccountA(symbol, sellPrice, amount, chatId);
            log.info("Цикл: Аккаунт A выставил лимитку на продажу: цена={}, сумма={}, ордер={}", sellPrice, amount, limitSellOrderId);

            // Аккаунт B выкупает лимитку
            mexcTradeService.buyFromAccountB(symbol, sellPrice, amount, chatId);
            log.info("Цикл: Аккаунт B купил у A по цене {} на сумму {}", sellPrice, amount);

            // Выставляем лимитку на покупку (Аккаунт A)
            BigDecimal buyPrice = mexcTradeService.getNearUpperSpreadPrice(symbol);
            String limitBuyOrderId = mexcTradeService.placeLimitBuyAccountA(symbol, buyPrice, amount, chatId);
            log.info("Цикл: Аккаунт A выставил лимитку на покупку: цена={}, сумма={}, ордер={}", buyPrice, amount, limitBuyOrderId);

            // Аккаунт B продаёт в лимитку A
            mexcTradeService.sellFromAccountB(symbol, buyPrice, amount, chatId);
            log.info("Цикл: Аккаунт B продал в лимитку A по цене {} на сумму {}", buyPrice, amount);

            log.info("✅ Цикл перелива успешно завершён");

        } catch (Exception e) {
            log.error("❌ Ошибка в цикле перелива", e);
            // если ошибка — распродаем на аккаунте А всё по рынку, чтобы не зависло
            try {
                mexcTradeService.forceMarketSellAccountA(symbol, amount, chatId);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}


