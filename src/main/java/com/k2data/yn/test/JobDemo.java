package com.k2data.yn.test;

import com.k2data.yn.test.flink.EventTimeAssigner;
import com.k2data.yn.test.flink.FillNA;
import com.k2data.yn.test.flink.OPCJsonDataParserMapFunction;
import com.k2data.yn.test.flink.TimerSource;
import com.k2data.yn.test.pojo.PointData;
import com.k2data.yn.test.rpc.XmlRPCUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JobDemo {

    private final static Logger LOGGER = LoggerFactory.getLogger(JobDemo.class);

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);


        // 控制参数
        String rpcChannel = "10.1.10.21:5568";

        String jobName = "modelId_deviceId_operatorId_001";
        Map<String, String> inputPoints = new HashMap<>();  //输入参数信息，物理测点名-基础算子形参名
        inputPoints.put("加载吨位", "payload");
        inputPoints.put("电机转速", "speed");
        Map<String, String> outputPoints = new HashMap<>();  //输出参数信息，输出参数名-输出参数类型
        outputPoints.put("running_healthy_result", "bool");
        String windowType = "count";  //count | time
        long windowStep = 5;
        long windowSize = 5;
        double payload_low_cut = 0;
        double payload_high_cut = 10000;
        double speed_low_cut = 0;
        double speed_high_cut = 10000;
        String inputType = "kafka";
        String inputBootstrapServers = "10.1.10.21:9092";
        String inputTopicName = "esf-wenfei";
        String outputType = "kafka";
        String outputBootstrapServers = "10.1.10.21:9092";
        String outputTopicName = "esf-wenfei-" + jobName;


        //**************
        //输入变量
        //**************

//        DataStream<PointData> inputStream1 = env.addSource(new CSVSource("data/断泵记录-2018-1-4.csv", 1000, Arrays.asList("加载吨位")));
//        DataStream<PointData> inputStream2 = env.addSource(new CSVSource("data/断泵记录-2018-1-4.csv", 1000, Arrays.asList("电机转速")));
        DataStream<PointData> inputStream1 = null;
        DataStream<PointData> inputStream2 = null;

        if ("kafka".equalsIgnoreCase(inputType)) {
            Properties consumerProperties = new Properties();
            consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, inputBootstrapServers);
            consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            //如果kafka集群开启了ssl，则使用下面的配置
////        consumerProperties.setProperty("ssl.keystore.location", AbnormalProcessing.class.getResource("/kafka_ssl/client.keystore.jks").getPath());
//        consumerProperties.setProperty("ssl.keystore.location", Thread.currentThread().getContextClassLoader().getResource("kafka_ssl/client.keystore.jks").getPath());
//        consumerProperties.setProperty("ssl.keystore.password", "K2DataKH");
////        consumerProperties.setProperty("ssl.truststore.location", AbnormalProcessing.class.getResource("/kafka_ssl/client.truststore.jks").getPath());
//        consumerProperties.setProperty("ssl.truststore.location", Thread.currentThread().getContextClassLoader().getResource("kafka_ssl/client.truststore.jks").getPath());
//        consumerProperties.setProperty("ssl.truststore.password", "K2DataKH");
//        consumerProperties.setProperty("ssl.key.password", "K2DataKH");
//        consumerProperties.setProperty("security.protocol", "SSL");

            FlinkKafkaConsumer011<String> inputConsumer = new FlinkKafkaConsumer011<String>(inputTopicName,
                    new SimpleStringSchema(),
                    consumerProperties);
            inputConsumer.setStartFromLatest();

            inputStream1 = env.addSource(inputConsumer)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> "加载吨位".equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());

            inputStream2 = env.addSource(inputConsumer)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> "电机转速".equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
        }


        //按秒补齐数据
        DataStream<PointData> heartbeatStream = env.addSource(new TimerSource(windowStep * 1000))
                .assignTimestampsAndWatermarks(new EventTimeAssigner());
        inputStream1 = new FillNA(windowStep, "加载吨位", heartbeatStream).doOperation(inputStream1)
                .assignTimestampsAndWatermarks(new EventTimeAssigner());
        inputStream2 = new FillNA(windowStep, "电机转速", heartbeatStream).doOperation(inputStream2)
                .assignTimestampsAndWatermarks(new EventTimeAssigner());

        //合并输入数据
        DataStream<List<PointData>> joinedInputStream = inputStream1
                .join(inputStream2)
                .where((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<PointData, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(PointData first, PointData second) throws Exception {
                        return Arrays.asList(first, second);
                    }
                });

        //设定算子执行窗口大小和步长
        AllWindowedStream<List<PointData>, ? extends Window> windowedStream = null;
        if ("count".equals(windowType)) {  //按数据个数设定窗口
            windowedStream = joinedInputStream
                    .countWindowAll(windowSize, windowStep);
        } else {  //按数据时间设定数据窗口
            windowedStream = joinedInputStream
                    .windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowStep)));
        }

        //调用RPC执行算子脚本
        DataStream<List<PointData>> rpcStream = windowedStream
                .aggregate(new AggregateFunction<List<PointData>, List<List<PointData>>, List<List<PointData>>>() {
                    @Override
                    public List<List<PointData>> createAccumulator() {
                        return new LinkedList<>();
                    }

                    @Override
                    public List<List<PointData>> add(List<PointData> value, List<List<PointData>> accumulator) {
                        int i = 0;
                        for (; i < accumulator.size(); i++) {
                            if (value.get(0).getTimestamp() < accumulator.get(i).get(0).getTimestamp()) {
                                break;
                            }
                        }
                        accumulator.add(i, value);
                        return accumulator;
                    }

                    @Override
                    public List<List<PointData>> getResult(List<List<PointData>> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public List<List<PointData>> merge(List<List<PointData>> a, List<List<PointData>> b) {
                        return null;
                    }
                })
                .keyBy((KeySelector<List<List<PointData>>, Long>) value -> 1L)  //这里构造一个假的key值，因为我们所有的计算在一个group中进行
                .map(new RichMapFunction<List<List<PointData>>, List<PointData>>() {

                    private transient ValueState<List<Object>> lastRPCResultState;  //上次算子执行的结果

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<List<Object>> descriptor =
                                new ValueStateDescriptor<>(
                                        "lastRPCResultState", // the state name
                                        TypeInformation.of(new TypeHint<List<Object>>() {
                                        }) // type information
                                );
                        lastRPCResultState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public List<PointData> map(List<List<PointData>> value) throws Exception {

                        //todo: 考虑这些信息如何从json中获取，且跟开头的inputPoints和outputPoints保持一致，或者考虑将他们合并存储？？
                        List<String> inputColumns = Arrays.asList("加载吨位", "电机转速");  //Python代码输入形参名，跟value中的pointdata顺序一致
                        List<String> outputColumns = Arrays.asList("running_healthy_result");

                        //输入数据
                        Map<String, List<Object>> data = new HashMap<>();
                        List<Object> index = new LinkedList<>();
                        for (String inputPointName : inputPoints.keySet()) {
                            data.put(inputPoints.get(inputPointName), new LinkedList<>());
                        }
                        for (List<PointData> row : value) {
                            for (String inputColumn : inputColumns) {
                                data.get(inputPoints.get(inputColumn))
                                        .add(row.get(inputColumns.indexOf(inputColumn)).getValue());
                            }
                            //todo: 解决rpc不支持Long类型传递的问题，保证时间精度
                            index.add((int) row.get(0).getTimestamp());  //每行数据的时间戳作为dataframe的index
                        }

                        //控制参数
                        Map<String, Object> kwargs = new HashMap<>();
                        kwargs.put("payload_low_cut", payload_low_cut);
                        kwargs.put("payload_high_cut", payload_high_cut);
                        kwargs.put("speed_low_cut", speed_low_cut);
                        kwargs.put("speed_high_cut", speed_high_cut);

                        //上次rpc运算结果
                        List<Object> lastRPCResult = lastRPCResultState.value();
                        if (lastRPCResult != null) {
                            for (String outputColumn : outputColumns) {
                                kwargs.put(outputColumn, lastRPCResult.get(outputColumns.indexOf(outputColumn)));
                            }
                        }

                        //调用RPC
                        Object[] response = XmlRPCUtils.callRPC(rpcChannel, "running_healthy", data, index, kwargs);

                        //更新计算结果到状态存储
                        lastRPCResultState.update(Arrays.asList(response));

                        //输出结果
                        long timestamp = value.get(0).get(0).getTimestamp();   //这里取每组数据的最小时间戳作为此次rpc计算结果的时间戳
                        List<PointData> result = new LinkedList<>();
                        for (String outputColumn : outputColumns) {
                            result.add(new PointData(outputColumn, timestamp,
                                    response[outputColumns.indexOf(outputColumn)],
                                    outputPoints.get(outputColumn)));  //TODO：这里得考虑结果为非double类型的情况
                        }
                        return result;
                    }
                });


        //存储结果
        if ("kafka".equalsIgnoreCase(outputType)) {
            Properties producerProperties = new Properties();
            producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, outputBootstrapServers);
            producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "3000");
            //如果kafka开启了ssl，可以使用如下配置
////        producerProperties.setProperty("ssl.keystore.location", AbnormalProcessing.class.getResource("/kafka_ssl/client.keystore.jks").getPath());
//        producerProperties.setProperty("ssl.keystore.location", Thread.currentThread().getContextClassLoader().getResource("kafka_ssl/client.keystore.jks").getPath());
//        producerProperties.setProperty("ssl.keystore.password", "K2DataKH");
////        producerProperties.setProperty("ssl.truststore.location", AbnormalProcessing.class.getResource("/kafka_ssl/client.truststore.jks").getPath());
//        producerProperties.setProperty("ssl.truststore.location", Thread.currentThread().getContextClassLoader().getResource("kafka_ssl/client.truststore.jks").getPath());
//        producerProperties.setProperty("ssl.truststore.password", "K2DataKH");
//        producerProperties.setProperty("ssl.key.password", "K2DataKH");
//        producerProperties.setProperty("security.protocol", "SSL");

            FlinkKafkaProducer011<String> resultProducer = new FlinkKafkaProducer011<String>(
                    outputTopicName,
                    new SimpleStringSchema(),
                    producerProperties,
                    Optional.empty()  //round-robin to all partitions
            );

            rpcStream.flatMap(new FlatMapFunction<List<PointData>, String>() {
                @Override
                public void flatMap(List<PointData> value, Collector<String> out) throws Exception {
                    for (PointData p : value) {
                        out.collect(p.dump());
                    }
                }
            }).addSink(resultProducer);
        }


        //执行作业
        env.execute("Demo");
    }

}
