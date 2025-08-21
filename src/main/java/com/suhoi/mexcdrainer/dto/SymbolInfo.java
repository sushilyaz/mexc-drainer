package com.suhoi.mexcdrainer.dto;
import lombok.Data;
import java.util.List;

@Data
public class SymbolInfo {
    private String symbol;
    private String status;
    private String baseAsset;
    private int baseAssetPrecision;
    private String quoteAsset;
    private int quotePrecision;
    private int quoteAssetPrecision;
    private String baseSizePrecision;         // "0.0001"
    private String quoteAmountPrecision;      // min order amount (quote)
    private String quoteAmountPrecisionMarket;// min market order (quote)
    private String maxQuoteAmountMarket;
    private List<String> orderTypes; // e.g. LIMIT, MARKET, IMMEDIATE_OR_CANCEL ...
    private String makerCommission;
    private String takerCommission;
}
