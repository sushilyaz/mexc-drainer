package com.suhoi.mexcdrainer.dto;
import lombok.Data;

@Data
public class OpenOrder {
    private String orderId;
    private String symbol;
    private String price;
    private String origQty;
    private String executedQty;
    private String status;
    private String side;
    private String type;
}
