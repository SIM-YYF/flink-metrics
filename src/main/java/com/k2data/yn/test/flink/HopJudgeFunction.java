package com.k2data.yn.test.flink;

import com.k2data.yn.test.pojo.PointData;
import com.k2data.yn.test.pojo.PointDataWithHop;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class HopJudgeFunction extends RichMapFunction<PointData, PointDataWithHop> {

    private ValueState<Object> lastValueState;
    private String deviceId;
    private String taskId;

    public HopJudgeFunction(String deviceId, String taskId) {
        this.deviceId = deviceId;
        this.taskId = taskId;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Object> stateDescriptor = new ValueStateDescriptor<Object>("last value state", Object.class);
        lastValueState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public PointDataWithHop map(PointData pointData) throws Exception {
        Object lastValue = lastValueState.value();

        PointDataWithHop pointDataWithHop = new PointDataWithHop();
        pointDataWithHop.setPointName(pointData.getPointName());
        pointDataWithHop.setTimestamp(pointData.getTimestamp());
        pointDataWithHop.setDeviceId(this.deviceId);
        pointDataWithHop.setTaskId(this.taskId);
        pointDataWithHop.setValue(pointData.getValue());
        pointDataWithHop.setType(pointData.getType());


        if (lastValue != null && !pointData.getValue().equals(lastValue)) { // 发生跳变
            if (pointData.getType().equals("double")) {
                double hopValue = (Double) pointData.getValue() - (Double) lastValue;
                pointDataWithHop.setValue(hopValue);
            } else {
                pointDataWithHop.setValue((Boolean) pointData.getValue() ? 1 : -1);
            }
            pointDataWithHop.setType("double");
        }


        lastValueState.update(pointData.getValue());

        return pointDataWithHop;
    }
}
