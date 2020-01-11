package com.k2data.yn.test.rpc;

import org.apache.xmlrpc.XmlRpcException;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XmlRPCTest {

    public static void main(String[] args) throws XmlRpcException, MalformedURLException {

        List<Object> values = Arrays.asList(1d, 2d, 3d, 4d, 5d);   //数据
        Map<String, List<Object>> data = new HashMap<>();
        data.put("payload", values);
        data.put("speed", values);
        data.put("pressure", values);
        data.put("flow", values);

        List<Object> times = Arrays.asList(1574232126, 1574232127, 1574232128, 1574232129, 1574232130);  //dataframe中的索引信息

        Map<String, Object> kwargs = new HashMap<>();  //算子控制参数
        kwargs.put("payload_low_cut", 0d);
        kwargs.put("payload_high_cut", 10000d);
        kwargs.put("speed_low_cut", 0d);
        kwargs.put("speed_high_cut", 10000d);
        kwargs.put("time_window", 5d);
        kwargs.put("pressure_gradient_cut", -0d);
        kwargs.put("flow_gradient_cut", -0d);
        kwargs.put("pressure_standard", 0.1d);
        kwargs.put("flow_standard", 0.1d);

//        long s = System.currentTimeMillis();
//        long count = 100;
//        for (int i = 0; i < count; i++) {

        //征兆算子
//        List<String> outputNames = Arrays.asList("running_healthy");
//        Object[] response = XmlRPCUtils.callRPC("running_healthy", data, times, kwargs);
//        List<String> outputNames = Arrays.asList("pressure_sensor_healthy");
//        Object[] response = XmlRPCUtils.callRPC("pressure_sensor_healthy", data, times, kwargs);
//        List<String> outputNames = Arrays.asList("flow_sensor_healthy");
//        Object[] response = XmlRPCUtils.callRPC("flow_sensor_healthy", data, times, kwargs);
//        List<String> outputNames = Arrays.asList("pressure_decrease_vastly");
//        Object[] response = XmlRPCUtils.callRPC("pressure_decrease_vastly", data, times, kwargs);
//        List<String> outputNames = Arrays.asList("flow_decrease_vastly");
//        Object[] response = XmlRPCUtils.callRPC("flow_decrease_vastly", data, times, kwargs);
//        List<String> outputNames = Arrays.asList("presssure_exceed_80threshold");
//        Object[] response = XmlRPCUtils.callRPC("pressure_threshold_judgement", data, times, kwargs);
//        List<String> outputNames = Arrays.asList("flow_exceed_90threshold");
//        Object[] response = XmlRPCUtils.callRPC("pressure_threshold_judgement", data, times, kwargs);

        //诊断（研判）算子
        Map<String, List<Object>> symptoms = new HashMap<>();
        symptoms.put("running_healthy", Arrays.asList(true));
        symptoms.put("pressure_sensor_healthy", Arrays.asList(true));
        symptoms.put("flow_sensor_healthy", Arrays.asList(true));
        symptoms.put("pressure_decrease_vastly", Arrays.asList(true));
        symptoms.put("flow_decrease_vastly", Arrays.asList(true));
        symptoms.put("presssure_exceed_80threshold", Arrays.asList(true));
        symptoms.put("flow_exceed_90threshold", Arrays.asList(true));

        kwargs.clear();
        kwargs.put("pump_break_flag", false);
        kwargs.put("pressure_standard", 0.1);
        kwargs.put("flow_standard", 0.1);
        kwargs.put("maybe_error", 0);
        kwargs.put("maybe_repair", 0);

        List<String> outputNames = Arrays.asList("pump_break_diagnosis", "pressure_standard", "flow_standard", "maybe_error", "maybe_repair");
        Object[] response = XmlRPCUtils.callRPC("10.1.10.21:5568", "pump_break_diagnosis", symptoms, times, kwargs);


//        }
//        long e = System.currentTimeMillis();
//        System.out.println("平均耗时: " + (e - s) / count + "ms");

        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < outputNames.size(); i++) {
            result.put(outputNames.get(i), response[i]);
        }

        System.out.println(result);
    }
}
