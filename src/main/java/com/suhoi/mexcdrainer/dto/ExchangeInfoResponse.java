package com.suhoi.mexcdrainer.dto;
import lombok.Data;
import java.util.List;

@Data
public class ExchangeInfoResponse {
    private String timezone;
    private long serverTime;
    private List<SymbolInfo> symbols;
}
