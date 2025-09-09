package com.suhoi.mexcdrainer.util;

import java.util.Locale;
import java.util.Objects;

public final class MexcChannels {
    private MexcChannels() {}
    private static String up(String s) { return Objects.requireNonNull(s).toUpperCase(Locale.ROOT); }

    public static String bookTicker(String symbol, int intervalMs) {
        return "spot@public.aggre.bookTicker.v3.api.pb@" + intervalMs + "ms@" + up(symbol);
    }

    public static String diffDepth(String symbol, int intervalMs) {
        return "spot@public.aggre.depth.v3.api.pb@" + intervalMs + "ms@" + up(symbol);
    }

    /** partial limit depth — 5/10/20 уровней */
    public static String partialDepth(String symbol, int levels) {
        return "spot@public.limit.depth.v3.api.pb@" + up(symbol) + "@" + levels;
    }
}
