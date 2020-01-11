package com.k2data.yn.test.pojo;

public class KafkaStorage extends Storage {
    private String bootstrapServers;
    private String topic;
    private String pointName;

    public KafkaStorage() {
    }

    public KafkaStorage(String bootstrapServers, String topic, String pointName) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.pointName = pointName;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPointName() {
        return pointName;
    }

    public void setPointName(String pointName) {
        this.pointName = pointName;
    }
}
