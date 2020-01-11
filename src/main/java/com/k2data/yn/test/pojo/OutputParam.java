package com.k2data.yn.test.pojo;

import java.io.Serializable;
import java.util.Map;

public class OutputParam implements Serializable {

    private String paramName;   //业务变量名
    private String type;   //参数值类型，double|bool
    private Map<String, Storage> storage;  //变量数据源存储信息

    public OutputParam() {
    }

    public OutputParam(String paramName, String type, Map<String, Storage> storage) {
        this.paramName = paramName;
        this.type = type;
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

    public Map<String, Storage> getStorage() {
        return storage;
    }

    public void setStorage(Map<String, Storage> storage) {
        this.storage = storage;
    }
}
