package com.k2data.yn.test.pojo;

import com.k2data.yn.test.common.KafkaUtils;
import com.k2data.yn.test.common.TimeUtils;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.Objects;

public class PointData {

    private String pointName;
    private long timestamp;
    private Object value;
    private String type;  //double|bool

    public PointData() {
    }

    public PointData(String pointName, long timestamp, Object value, String type) {
        this.pointName = pointName;
        this.timestamp = timestamp;
        this.value = value;
        this.type = type;
    }

    public String getPointName() {
        return pointName;
    }

    public void setPointName(String pointName) {
        this.pointName = pointName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * dump for storing into kafka.
     *
     * @return
     */
    public String dump() {
//        return pointName + "," + timestamp + "," + value;
        return KafkaUtils.convertDataToJson(timestamp, pointName, value).toString();

    }

    /**
     * load from string into fields.
     */
    public void load(JSONObject obj) {
//        String[] items = s.split(",");
//        if (items.length < 3) {
//            return;
//        }
//        this.pointName = items[0];
//        this.timestamp = Long.parseLong(items[1]);
//        this.value = Double.parseDouble(items[2]);

        this.pointName = obj.getJSONObject("tags").getString("node_id");
        this.timestamp = TimeUtils.convertToSeconds(obj.getLong("time")) * 1000;
        this.value = obj.getJSONObject("fields").get("value");
        try {
            Double.parseDouble(value.toString());
            this.type = "double";
        } catch (NumberFormatException e) {
            this.type = "bool";
        }
    }

    @Override
    public String toString() {
        return "PointData{" +
                "pointName='" + pointName + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                ", value=" + value +
                ", type='" + type + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PointData pointData = (PointData) o;
        return timestamp == pointData.timestamp &&
                Objects.equals(pointName, pointData.pointName) &&
                Objects.equals(value, pointData.value) &&
                Objects.equals(type, pointData.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pointName, timestamp, value, type);
    }
}
