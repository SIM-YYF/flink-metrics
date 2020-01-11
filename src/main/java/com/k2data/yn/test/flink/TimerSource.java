package com.k2data.yn.test.flink;

import com.k2data.yn.test.pojo.PointData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 按找固定时间间隔发送心跳数据.
 */
public class TimerSource implements SourceFunction<PointData> {

    private boolean running = true;
    private long intervalMs;

    public TimerSource(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    @Override
    public void run(SourceContext<PointData> ctx) throws Exception {
        while (running) {
            ctx.collect(new PointData(TimerSource.class.getSimpleName(), System.currentTimeMillis(), Double.NaN, "double"));
            Thread.sleep(intervalMs);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
