package com.k2data.yn.test;

import com.k2data.yn.test.common.KafkaUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataMocker {

    public static void main(String[] args) throws IOException, InterruptedException {

        final ParameterTool params = ParameterTool.fromArgs(args);

//        List<String> columns = Arrays.asList("load_tonnage", "AC5000PZD_in3", "P17_YL", "F12");
        String columnNames = params.getRequired("columns");
        List<String> columns = Arrays.asList(columnNames.split(","));

//        String dataFile = "data/断泵记录-2018-1-4.csv";
        String dataFile = params.getRequired("dataFile");
        long intervalMs = params.getLong("intervalMs", 1000);
        long roundNum = params.getLong("roundNum", 1);
        boolean resetTime = params.getBoolean("resetTime", true);
        String bootstrapServers = params.getRequired("bootstrapServers");
        String kafkaTopic = params.getRequired("kafkaTopic");

        KafkaUtils.generateMockData(dataFile, intervalMs, columns, roundNum, resetTime, bootstrapServers, kafkaTopic);

    }
}
