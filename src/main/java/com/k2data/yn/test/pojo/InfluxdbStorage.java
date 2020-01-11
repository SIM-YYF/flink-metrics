package com.k2data.yn.test.pojo;

public class InfluxdbStorage extends Storage {
    private String url;
    private String user;
    private String password;
    private String database;
    private String measurement;
    private String fieldName;

    public InfluxdbStorage() {
    }

    public InfluxdbStorage(String url, String user, String password, String database, String measurement, String fieldName) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.database = database;
        this.measurement = measurement;
        this.fieldName = fieldName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getMeasurement() {
        return measurement;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
