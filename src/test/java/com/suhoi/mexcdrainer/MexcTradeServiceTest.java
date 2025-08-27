package com.suhoi.mexcdrainer;

import com.suhoi.mexcdrainer.service.MexcTradeService;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;

public class MexcTradeServiceTest {

    // хардкод ключей для теста
    private static final String API_KEY = "mx0vglKvKt55AVJwDs";
    private static final String SECRET_KEY = "2129a7877d774aeb90701ca3f6b02160";

    private final MexcTradeService tradeService = new MexcTradeService();

//    @Test
//    public void testMarketBuy() {
//        String symbol = "ANTUSDT";      // тестовый символ
//        BigDecimal usdtAmount = BigDecimal.valueOf(2); // тестовая сумма
//
//        try {
//            // напрямую вызываем метод, минуя MemoryDb
//            String orderId = tradeService.signedMarketBuy(symbol, usdtAmount, API_KEY, SECRET_KEY);
//            System.out.println("Успешно создан market buy, orderId=" + orderId);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}

