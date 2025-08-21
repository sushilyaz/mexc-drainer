package com.suhoi.mexcdrainer.dto;
import lombok.Data;

@Data
public class OrderResponse {
    private String orderId;
    private String clientOrderId;
    private String symbol;
    private String status;         // NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED
    private String side;
    private String type;
    private String price;
    private String origQty;
    private String executedQty;
    private String cummulativeQuoteQty;
}
