package com.k2data.yn.test.pojo;

import java.io.Serializable;
import java.util.Map;

public class InputParam implements Serializable {

    private String paramName;  //业务变量名
    private String type;  //传感器值类型，double|bool
    private long intervalMs; //传感器发送间隔，单位毫秒
    private boolean fill;  //是否补齐（按秒）
    private Map<String, Storage> storage;  //变量数据源存储信息

    public InputParam() {
    }

    public InputParam(String paramName, String type, long intervalMs, boolean fill, Map<String, Storage> storage) {
        this.paramName = paramName;
        this.type = type;
        this.intervalMs = intervalMs;
        this.fill = fill;
        this.storage = storage;
    }

    public String getParamName() {
        return paramName;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    public boolean isFill() {
        return fill;
    }

    public void setFill(boolean fill) {
        this.fill = fill;
    }

    public Map<String, Storage> getStorage() {
        return storage;
    }

    public void setStorage(Map<String, Storage> storage) {
        this.storage = storage;
    }
}
