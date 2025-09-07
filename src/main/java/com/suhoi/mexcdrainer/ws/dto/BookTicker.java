package com.suhoi.mexcdrainer.ws.dto;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;

/**
 * Последний лучший бид/оффер по символу из WS.
 */
@Value
@Builder
public class BookTicker {
    String symbol;
    BigDecimal bidPrice;
    BigDecimal bidQuantity;
    BigDecimal askPrice;
    BigDecimal askQuantity;
    long sendTime; // миллисекунды от биржи
}
