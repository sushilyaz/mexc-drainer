package com.suhoi.mexcdrainer.dto;

import lombok.Builder;
import lombok.Data;

@Data @Builder
public class NewOrderRequest {
    private String symbol;     // e.g. ANTUSDT
    private String side;       // BUY / SELL
    private String type;       // LIMIT / MARKET / IMMEDIATE_OR_CANCEL / FILL_OR_KILL / POST_ONLY
    private String quantity;   // base units (for LIMIT)
    private String quoteOrderQty; // quote spend (for MARKET BUY)
    private String price;      // quote price (for LIMIT)
    private String newClientOrderId;
}
