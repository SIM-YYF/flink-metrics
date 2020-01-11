package com.k2data.yn.test.flink;

import com.k2data.yn.test.pojo.PointData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OPCJsonDataParserMapFunction implements FlatMapFunction<String, PointData> {
    private final static Logger LOGGER = LoggerFactory.getLogger(OPCJsonDataParserMapFunction.class);

    @Override
    public void flatMap(String s, Collector<PointData> collector) throws Exception {
        try {
            JSONArray opcItems = new JSONArray(s);
            for (int i = 0; i < opcItems.length(); i++) {
                JSONObject obj = (JSONObject) opcItems.get(i);
                PointData pointData = new PointData();
                pointData.load(obj);
                collector.collect(pointData);
            }
        } catch (JSONException je) {
            LOGGER.warn("Invalid OPC JSON data: {} - {}", je.getMessage(), s);
        }
    }
}
