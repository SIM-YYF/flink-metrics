package com.k2data.yn.test.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TimeUtils {

    private final static DateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final static DateFormat pstFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    static {
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        pstFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
    }


    /**
     * 将时间精度控制到秒。
     *
     * @param time
     * @return
     */
    public static long convertToSeconds(long time) {
        int timeLength = (int) (Math.log10(time) + 1);
        if (timeLength > 10) {  //如果大于秒级数据的长度
            return (long) (time / Math.pow(10, timeLength - 10));
        }
        return time;
    }

    /**
     * 将1min, 1sec等描述翻译为对应的秒数。
     *
     * @param time
     * @return
     */
    public static long translateTime(String time) {
        if (time.endsWith("day")) {
            time = time.substring(0, time.lastIndexOf("day"));
            return 24 * 3600 * Long.parseLong(time);
        }
        if (time.endsWith("hour")) {
            time = time.substring(0, time.lastIndexOf("hour"));
            return 3600 * Long.parseLong(time);
        }
        if (time.endsWith("min")) {
            time = time.substring(0, time.lastIndexOf("min"));
            return 60 * Long.parseLong(time);
        }
        if (time.endsWith("sec")) {
            time = time.substring(0, time.lastIndexOf("sec"));
            return Long.parseLong(time);
        }
        return Long.MIN_VALUE;
    }

    /**
     * 将时间字符串翻译到毫秒，且转化为东八区时间。
     *
     * @param time 如 "2019-11-11 10:08:02" （输入是0时区）
     * @return
     */
    public static long parseTimeToMs(String time) throws ParseException {
        Date date = utcFormat.parse(time);
        String pstString = pstFormat.format(date);
        Date date2 = pstFormat.parse(pstString);
        return date2.getTime();
    }
}
