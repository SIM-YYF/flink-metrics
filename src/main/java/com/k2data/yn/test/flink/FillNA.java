package com.k2data.yn.test.flink;

import com.k2data.yn.test.common.TimeUtils;
import com.k2data.yn.test.pojo.PointData;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 按秒补齐数据点。
 */
public class FillNA implements IDataAnalyseOperator<DataStream<PointData>, DataStream<PointData>>, Serializable {
    private final static Logger LOGGER = LoggerFactory.getLogger(FillNA.class);

    private long windowSizeSecs;  //窗体大小，这里只能使用Tumbling窗体
    private String pointName;  //指定需要被补齐数据的测点名称
    transient DataStream<PointData> heartbeatStream;  //心跳数据流，用于解决原始数据长时间不更新的问题

    private long DATA_MAX_LATENCY_SEC = 60;  //原始数据的 max(processingTime - eventTime)，也就是最大延迟

    public FillNA(long windowSizeSecs, String pointName, DataStream<PointData> heartbeatStream) {
        this.windowSizeSecs = windowSizeSecs;
        this.pointName = pointName;
        this.heartbeatStream = heartbeatStream;
    }

    @Override
    public DataStream<PointData> doOperation(DataStream<PointData> pointDataDataStream) {
        if (pointName == null) {
            LOGGER.error("Not specify the point name to be filled!");
            return null;
        }

        SingleOutputStreamOperator<PointData> singleOutputStreamOperator = pointDataDataStream
                .filter((FilterFunction<PointData>) pointData -> pointName.equals(pointData.getPointName()));

        DataStream<PointData> result = singleOutputStreamOperator
                .union(heartbeatStream)
                .keyBy((KeySelector<PointData, Long>) value -> 1L) //构造key用于将所有数据汇集到一个流
                .flatMap(new RichFlatMapFunction<PointData, PointData>() {
                    private transient ValueState<PointData> lastState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<PointData> descriptor =
                                new ValueStateDescriptor<>(
                                        "lastState", // the state name
                                        TypeInformation.of(new TypeHint<PointData>() {
                                        }) // type information
                                );
                        lastState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void flatMap(PointData value, Collector<PointData> out) throws Exception {

                        PointData lastData = lastState.value();  //最后一次接收到的原始数据

                        if (isHeartBeat(value)) {
                            if (lastData == null) {
                                return;
                            } else {
                                if (value.getTimestamp() - lastData.getTimestamp() > DATA_MAX_LATENCY_SEC * 1000 &&
                                        value.getTimestamp() - lastData.getTimestamp() <= 1800000) {  //心跳间隔不能大于1800秒，否则就认定为历史数据接入，不按照当前心跳时间补齐
                                    //如果在DATA_MAX_LATENCY_SEC时间内都没有原始数据接入，则将数据补齐到 (heartBeat.getTimestamp - DATA_MAX_LATENCY_SEC)
                                    long timeIndexSec = TimeUtils.convertToSeconds(lastData.getTimestamp()) + 1;
                                    long heartBeatTimeSecs = TimeUtils.convertToSeconds(value.getTimestamp());
                                    while (timeIndexSec <= heartBeatTimeSecs) {
                                        PointData p = new PointData(pointName, timeIndexSec * 1000, lastData.getValue(), lastData.getType());
                                        out.collect(p);
                                        timeIndexSec++;
                                    }
                                    lastState.update(new PointData(pointName, (timeIndexSec - 1) * 1000, lastData.getValue(), lastData.getType()));
                                }
                            }
                        } else {
                            if (lastData != null) {

                                //当遇到数据延迟大于 DATA_MAX_LATENCY_SEC 的情况时，lastData会被heartBeat更新，导致value.getTimestamp() < lastData.getTimestamp()
                                //此时将 DATA_MAX_LATENCY_SEC 增大一倍
                                if (value.getTimestamp() < lastData.getTimestamp()) {
                                    DATA_MAX_LATENCY_SEC = DATA_MAX_LATENCY_SEC * 2;
                                    LOGGER.warn("Received late data and increase DATA_MAX_LATENCY_SEC to {} secs", DATA_MAX_LATENCY_SEC);
                                }

                                //补齐lastData到当前数据之间的数据
                                long timeIndexSec = TimeUtils.convertToSeconds(lastData.getTimestamp()) + 1;
                                long currentTimeSecs = TimeUtils.convertToSeconds(value.getTimestamp());
                                while (timeIndexSec < currentTimeSecs) {
                                    PointData p = new PointData(pointName, timeIndexSec * 1000, lastData.getValue(), lastData.getType());
                                    out.collect(p);
                                    timeIndexSec++;
                                }
                            }
                            out.collect(value);
                            lastState.update(value);
                        }
                    }
                })
                .uid(getClass().getSimpleName() + "-" + pointName).name(getClass().getSimpleName() + "-" + pointName);

        return result;
    }


    private boolean isHeartBeat(PointData pointData) {
        return pointData.getValue() instanceof Double && Double.isNaN((Double) pointData.getValue());
    }
}
