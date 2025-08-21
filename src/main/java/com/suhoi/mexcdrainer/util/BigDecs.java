package com.suhoi.mexcdrainer.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class BigDecs {
    public static BigDecimal bd(double d) { return new BigDecimal(Double.toString(d)); }

    public static BigDecimal floorToStep(BigDecimal value, BigDecimal step) {
        if (step == null || step.signum() == 0) return value;
        BigDecimal n = value.divide(step, 0, RoundingMode.DOWN);
        return n.multiply(step);
    }

    public static int scaleFromStepString(String step) {
        // "0.0001" -> 4
        int idx = step.indexOf('.');
        return idx < 0 ? 0 : (step.length() - idx - 1);
    }

    public static BigDecimal parse(String s) { return new BigDecimal(s); }
}
