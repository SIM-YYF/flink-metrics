package com.k2data.yn.test.common;

public class ValueUtils {

    public static double anyToDouble(Object o, String type) {
        double d = Double.NaN;

        if ("double".equalsIgnoreCase(type)) {
            d = Double.parseDouble(o.toString());
        } else if ("bool".equalsIgnoreCase(type)) {
            d = Boolean.parseBoolean(o.toString()) ? 1 : 0;
        }

        return d;
    }
}
