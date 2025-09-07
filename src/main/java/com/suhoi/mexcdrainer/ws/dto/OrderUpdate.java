package com.suhoi.mexcdrainer.ws.dto;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;

/**
 * Апдейт по ордеру из приватного WS.
 * См. spot@private.orders.v3.api.pb (status: 1 NEW, 2 FILLED, 3 PARTIAL, 4 CANCELED, 5 PARTIAL_CANCELED).
 */
@Value
@Builder
public class OrderUpdate {
    String symbol;
    String orderId;        // privateOrders.orderId (у MEXC иногда это "clientId")
    String clientOrderId;  // privateOrders.clientId
    BigDecimal price;      // строка -> BigDecimal
    BigDecimal quantity;   // из privateOrders.quantity
    BigDecimal avgPrice;   // privateOrders.avgPrice
    BigDecimal cumulativeQuantity; // privateOrders.cumulativeQuantity
    int status;            // 1..5
    long sendTime;

    public boolean isFilled() { return status == 2; }
    public boolean isCanceled() { return status == 4 || status == 5; }
}
