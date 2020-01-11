package com.k2data.yn.test.pojo;

import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.Objects;

public class PointDataWithHop extends PointData {

    private String taskId = "";
    private String deviceId = "";

    public PointDataWithHop() {
    }

    public PointDataWithHop(String pointName, long timestamp, Object value, String type, String taskId, String deviceId) {
        super(pointName, timestamp, value, type);
        this.taskId = taskId;
        this.deviceId = deviceId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }


    /**
     * dump for storing into kafka.
     *
     * @return
     */
    public String dump() {
        JSONObject obj = new JSONObject();
        obj.put("paramName", this.getPointName());
        obj.put("taskId", this.taskId);
        obj.put("deviceId", this.deviceId);
        obj.put("timestamp", this.getTimestamp());
        obj.put("hopValue", this.getValue());
        return obj.toString();

    }


    @Override
    public String toString() {
        return "PointDataWithHop{" +
                "pointName='" + this.getPointName() + '\'' +
                ", timestamp=" + new Timestamp(this.getTimestamp()) +
                ", value=" + this.getValue() +
                ", type='" + this.getType() + '\'' +
                ", taskId='" + this.taskId + '\'' +
                ", deviceId='" + this.deviceId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PointDataWithHop pointData = (PointDataWithHop) o;
        return this.getTimestamp() == pointData.getTimestamp() &&
                Objects.equals(this.getPointName(), pointData.getPointName()) &&
                Objects.equals(this.getValue(), pointData.getValue()) &&
                Objects.equals(this.getType(), pointData.getType()) &&
                Objects.equals(this.taskId, pointData.getTaskId()) &&
                Objects.equals(this.deviceId, pointData.getDeviceId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getPointName(), this.getTimestamp(), this.getValue(), this.getType(), this.getTaskId(), this.getDeviceId());
    }
}
