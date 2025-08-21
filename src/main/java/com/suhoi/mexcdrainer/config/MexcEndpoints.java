package com.suhoi.mexcdrainer.config;

public final class MexcEndpoints {
    private MexcEndpoints() {}
    public static final String BOOK_TICKER = "/api/v3/ticker/bookTicker";
    public static final String DEPTH       = "/api/v3/depth";
    public static final String EXCHANGE_INFO = "/api/v3/exchangeInfo";
    public static final String ACCOUNT     = "/api/v3/account";
    public static final String TRADE_FEE   = "/api/v3/tradeFee";
    public static final String ORDER       = "/api/v3/order";
    public static final String QUERY_ORDER = "/api/v3/order";
    public static final String CANCEL_ORDER= "/api/v3/order";
}
