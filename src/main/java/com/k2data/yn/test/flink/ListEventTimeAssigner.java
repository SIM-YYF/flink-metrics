package com.k2data.yn.test.flink;

import com.k2data.yn.test.pojo.PointData;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.util.List;

public class ListEventTimeAssigner implements AssignerWithPeriodicWatermarks<List<PointData>> {
    private final long MAX_OUT_OF_ORDERNESS = 3500; // 3.5 secs
    private final int TIMESTAMP_LENGTH = 13; // digits number for milliseconds timestamp
    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - MAX_OUT_OF_ORDERNESS);
    }

    @Override
    public long extractTimestamp(List<PointData> element, long l) {
        long timestamp = element.get(0).getTimestamp();
        // Flink内部使用ms做时间单位，这里需要转换
        int length = (int) (Math.log10(timestamp) + 1);
        if (length > TIMESTAMP_LENGTH) {
            timestamp /= (long) Math.pow(10, length - TIMESTAMP_LENGTH);
        } else if (length < TIMESTAMP_LENGTH) {
            timestamp *= (long) Math.pow(10, TIMESTAMP_LENGTH - length);
        }

        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
