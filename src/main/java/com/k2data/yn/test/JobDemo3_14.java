package com.k2data.yn.test;

import com.k2data.yn.test.common.GlobalConfig;
import com.k2data.yn.test.flink.*;
import com.k2data.yn.test.pojo.*;
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

public class JobDemo3_14 {

    private final static Logger LOGGER = LoggerFactory.getLogger(JobDemo3_14.class);

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

        String jobName = JobDemo3_14.class.getSimpleName();
        Map<String, String> tags = new HashMap<>();
        tags.put("deviceId", "device_11001");

        String operatorName = "diagnosis_pump_break";

        String windowType = GlobalConfig.WINDOW_TYPE;  //count | time
        long windowSize = 1;
        long windowStep = 1;

        //控制参数
        Map<String, Object> kwargs = new HashMap<>();
        kwargs.put("pump_break_flag", false);
        kwargs.put("maybe_error", 0);
        kwargs.put("maybe_repair", 0);

        String inputType = "kafka";
        List<InputParam> inputParams = new LinkedList<>();
        Map<String, Storage> storage1 = new HashMap<>();
        storage1.put(inputType, new KafkaStorage("10.1.10.21:9092", "esf-wenfei", "P17_YL"));
        inputParams.add(new InputParam("pressure", "double", 1000, false, storage1));
        Map<String, Storage> storage2 = new HashMap<>();
        storage2.put(inputType, new KafkaStorage("10.1.10.21:9092", "esf-wenfei", "F12"));
        inputParams.add(new InputParam("flow", "double", 1000, false, storage2));
        Map<String, Storage> storage3 = new HashMap<>();
        storage3.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_1", "var_standard_recently"));
        inputParams.add(new InputParam("pressure_standard_recently", "double", 1000, false, storage3));
        Map<String, Storage> storage4 = new HashMap<>();
        storage4.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_2", "var_standard_recently"));
        inputParams.add(new InputParam("flow_standard_recently", "double", 1000, false, storage4));
        Map<String, Storage> storage5 = new HashMap<>();
        storage5.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_5", "running_healthy_result"));
        inputParams.add(new InputParam("running_healthy_result", "bool", 1000, false, storage5));
        Map<String, Storage> storage6 = new HashMap<>();
        storage6.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_6", "pressure_sensor_healthy_result"));
        inputParams.add(new InputParam("pressure_sensor_healthy_result", "bool", 1000, false, storage6));
        Map<String, Storage> storage7 = new HashMap<>();
        storage7.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_7", "flow_sensor_healthy_result"));
        inputParams.add(new InputParam("flow_sensor_healthy_result", "bool", 1000, false, storage7));
        Map<String, Storage> storage8 = new HashMap<>();
        storage8.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_8", "pressure_decrease_vastly_result"));
        inputParams.add(new InputParam("pressure_decrease_vastly_result", "bool", 1000, false, storage8));
        Map<String, Storage> storage9 = new HashMap<>();
        storage9.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_9", "flow_decrease_vastly_result"));
        inputParams.add(new InputParam("flow_decrease_vastly_result", "bool", 1000, false, storage9));
        Map<String, Storage> storage10 = new HashMap<>();
        storage10.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_10", "pressure_greater_than_80standard_result"));
        inputParams.add(new InputParam("pressure_greater_than_80standard_result", "bool", 1000, false, storage10));
        Map<String, Storage> storage11 = new HashMap<>();
        storage11.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_11", "flow_greater_than_90standard_result"));
        inputParams.add(new InputParam("flow_greater_than_90standard_result", "bool", 1000, false, storage11));
        Map<String, Storage> storage12 = new HashMap<>();
        storage12.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_12", "pressure_less_than_50standard_result"));
        inputParams.add(new InputParam("pressure_less_than_50standard_result", "bool", 1000, false, storage12));
        Map<String, Storage> storage13 = new HashMap<>();
        storage13.put(inputType, new KafkaStorage("10.1.10.21:9092", "JobDemo3_13", "flow_less_than_60standard_result"));
        inputParams.add(new InputParam("flow_less_than_60standard_result", "bool", 1000, false, storage13));


        String outputType = "kafka";
        List<OutputParam> outputParams = new LinkedList<>();
        Map<String, Storage> outputStorage1 = new HashMap<>();
//        outputStorage1.put(outputType, new KafkaStorage("10.1.10.21:9092", jobName, "pump_break_flag"));
        outputStorage1.put("influxdb", new InfluxdbStorage("http://10.1.10.21:8086", "k2data", "K2data1234", "esf", "device_11001_result_yntest", "pump_break_flag"));
        outputParams.add(new OutputParam("pump_break_flag", "bool", outputStorage1));
        Map<String, Storage> outputStorage2 = new HashMap<>();
//        outputStorage1.put(outputType, new KafkaStorage("10.1.10.21:9092", jobName, "pump_break_flag"));
        outputStorage2.put("influxdb", new InfluxdbStorage("http://10.1.10.21:8086", "k2data", "K2data1234", "esf", "device_11001_result_yntest", "maybe_error"));
        outputParams.add(new OutputParam("maybe_error", "double", outputStorage2));
        Map<String, Storage> outputStorage3 = new HashMap<>();
//        outputStorage1.put(outputType, new KafkaStorage("10.1.10.21:9092", jobName, "pump_break_flag"));
        outputStorage3.put("influxdb", new InfluxdbStorage("http://10.1.10.21:8086", "k2data", "K2data1234", "esf", "device_11001_result_yntest", "maybe_repair"));
        outputParams.add(new OutputParam("maybe_repair", "double", outputStorage3));


        //**************
        //输入变量
        //**************

        DataStream<PointData> inputStream1 = null;
        DataStream<PointData> inputStream2 = null;
        DataStream<PointData> inputStream3 = null;
        DataStream<PointData> inputStream4 = null;
        DataStream<PointData> inputStream5 = null;
        DataStream<PointData> inputStream6 = null;
        DataStream<PointData> inputStream7 = null;
        DataStream<PointData> inputStream8 = null;
        DataStream<PointData> inputStream9 = null;
        DataStream<PointData> inputStream10 = null;
        DataStream<PointData> inputStream11 = null;
        DataStream<PointData> inputStream12 = null;
        DataStream<PointData> inputStream13 = null;

        //按秒补齐数据
        DataStream<PointData> heartbeatStream = env.addSource(new TimerSource(windowStep * 1000))
                .assignTimestampsAndWatermarks(new EventTimeAssigner());

        if ("kafka".equalsIgnoreCase(inputType)) {
            Properties consumerProperties1 = new Properties();
            KafkaStorage kafkaStorage1 = (KafkaStorage) inputParams.get(0).getStorage().get(inputType);
            consumerProperties1.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage1.getBootstrapServers());
            consumerProperties1.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer1 = new FlinkKafkaConsumer011<String>(kafkaStorage1.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties1);
            inputConsumer1.setStartFromLatest();
            inputStream1 = env.addSource(inputConsumer1)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage1.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(0).isFill()) {
                inputStream1 = new FillNA(windowStep, kafkaStorage1.getPointName(), heartbeatStream).doOperation(inputStream1)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties2 = new Properties();
            KafkaStorage kafkaStorage2 = (KafkaStorage) inputParams.get(1).getStorage().get(inputType);
            consumerProperties2.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage2.getBootstrapServers());
            consumerProperties2.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer2 = new FlinkKafkaConsumer011<String>(kafkaStorage2.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties2);
            inputConsumer2.setStartFromLatest();
            inputStream2 = env.addSource(inputConsumer2)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage2.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(1).isFill()) {
                inputStream2 = new FillNA(windowStep, kafkaStorage2.getPointName(), heartbeatStream).doOperation(inputStream2)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties3 = new Properties();
            KafkaStorage kafkaStorage3 = (KafkaStorage) inputParams.get(2).getStorage().get(inputType);
            consumerProperties3.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage3.getBootstrapServers());
            consumerProperties3.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer3 = new FlinkKafkaConsumer011<String>(kafkaStorage3.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties3);
            inputConsumer3.setStartFromLatest();
            inputStream3 = env.addSource(inputConsumer3)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage3.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(2).isFill()) {
                inputStream3 = new FillNA(windowStep, kafkaStorage3.getPointName(), heartbeatStream).doOperation(inputStream3)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties4 = new Properties();
            KafkaStorage kafkaStorage4 = (KafkaStorage) inputParams.get(3).getStorage().get(inputType);
            consumerProperties4.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage4.getBootstrapServers());
            consumerProperties4.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer4 = new FlinkKafkaConsumer011<String>(kafkaStorage4.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties4);
            inputConsumer4.setStartFromLatest();
            inputStream4 = env.addSource(inputConsumer4)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage4.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(3).isFill()) {
                inputStream4 = new FillNA(windowStep, kafkaStorage4.getPointName(), heartbeatStream).doOperation(inputStream4)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties5 = new Properties();
            KafkaStorage kafkaStorage5 = (KafkaStorage) inputParams.get(4).getStorage().get(inputType);
            consumerProperties5.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage5.getBootstrapServers());
            consumerProperties5.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer5 = new FlinkKafkaConsumer011<String>(kafkaStorage5.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties5);
            inputConsumer5.setStartFromLatest();
            inputStream5 = env.addSource(inputConsumer5)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage5.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(4).isFill()) {
                inputStream5 = new FillNA(windowStep, kafkaStorage5.getPointName(), heartbeatStream).doOperation(inputStream5)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties6 = new Properties();
            KafkaStorage kafkaStorage6 = (KafkaStorage) inputParams.get(5).getStorage().get(inputType);
            consumerProperties6.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage6.getBootstrapServers());
            consumerProperties6.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer6 = new FlinkKafkaConsumer011<String>(kafkaStorage6.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties6);
            inputConsumer6.setStartFromLatest();
            inputStream6 = env.addSource(inputConsumer6)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage6.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(5).isFill()) {
                inputStream6 = new FillNA(windowStep, kafkaStorage6.getPointName(), heartbeatStream).doOperation(inputStream6)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties7 = new Properties();
            KafkaStorage kafkaStorage7 = (KafkaStorage) inputParams.get(6).getStorage().get(inputType);
            consumerProperties7.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage7.getBootstrapServers());
            consumerProperties7.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer7 = new FlinkKafkaConsumer011<String>(kafkaStorage7.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties7);
            inputConsumer7.setStartFromLatest();
            inputStream7 = env.addSource(inputConsumer7)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage7.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(6).isFill()) {
                inputStream7 = new FillNA(windowStep, kafkaStorage7.getPointName(), heartbeatStream).doOperation(inputStream7)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties8 = new Properties();
            KafkaStorage kafkaStorage8 = (KafkaStorage) inputParams.get(7).getStorage().get(inputType);
            consumerProperties8.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage8.getBootstrapServers());
            consumerProperties8.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer8 = new FlinkKafkaConsumer011<String>(kafkaStorage8.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties8);
            inputConsumer8.setStartFromLatest();
            inputStream8 = env.addSource(inputConsumer8)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage8.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(7).isFill()) {
                inputStream8 = new FillNA(windowStep, kafkaStorage8.getPointName(), heartbeatStream).doOperation(inputStream8)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties9 = new Properties();
            KafkaStorage kafkaStorage9 = (KafkaStorage) inputParams.get(8).getStorage().get(inputType);
            consumerProperties9.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage9.getBootstrapServers());
            consumerProperties9.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer9 = new FlinkKafkaConsumer011<String>(kafkaStorage9.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties9);
            inputConsumer9.setStartFromLatest();
            inputStream9 = env.addSource(inputConsumer9)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage9.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(8).isFill()) {
                inputStream9 = new FillNA(windowStep, kafkaStorage9.getPointName(), heartbeatStream).doOperation(inputStream9)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties10 = new Properties();
            KafkaStorage kafkaStorage10 = (KafkaStorage) inputParams.get(9).getStorage().get(inputType);
            consumerProperties10.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage10.getBootstrapServers());
            consumerProperties10.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer10 = new FlinkKafkaConsumer011<String>(kafkaStorage10.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties10);
            inputConsumer10.setStartFromLatest();
            inputStream10 = env.addSource(inputConsumer10)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage10.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(9).isFill()) {
                inputStream10 = new FillNA(windowStep, kafkaStorage10.getPointName(), heartbeatStream).doOperation(inputStream10)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties11 = new Properties();
            KafkaStorage kafkaStorage11 = (KafkaStorage) inputParams.get(10).getStorage().get(inputType);
            consumerProperties11.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage11.getBootstrapServers());
            consumerProperties11.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer11 = new FlinkKafkaConsumer011<String>(kafkaStorage11.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties11);
            inputConsumer11.setStartFromLatest();
            inputStream11 = env.addSource(inputConsumer11)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage11.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(10).isFill()) {
                inputStream11 = new FillNA(windowStep, kafkaStorage11.getPointName(), heartbeatStream).doOperation(inputStream11)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties12 = new Properties();
            KafkaStorage kafkaStorage12 = (KafkaStorage) inputParams.get(11).getStorage().get(inputType);
            consumerProperties12.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage12.getBootstrapServers());
            consumerProperties12.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer12 = new FlinkKafkaConsumer011<String>(kafkaStorage12.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties12);
            inputConsumer12.setStartFromLatest();
            inputStream12 = env.addSource(inputConsumer12)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage12.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(11).isFill()) {
                inputStream12 = new FillNA(windowStep, kafkaStorage12.getPointName(), heartbeatStream).doOperation(inputStream12)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }

            Properties consumerProperties13 = new Properties();
            KafkaStorage kafkaStorage13 = (KafkaStorage) inputParams.get(12).getStorage().get(inputType);
            consumerProperties13.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorage13.getBootstrapServers());
            consumerProperties13.setProperty(ConsumerConfig.GROUP_ID_CONFIG, jobName + "-consumer");
            FlinkKafkaConsumer011<String> inputConsumer13 = new FlinkKafkaConsumer011<String>(kafkaStorage13.getTopic(),
                    new SimpleStringSchema(),
                    consumerProperties13);
            inputConsumer13.setStartFromLatest();
            inputStream13 = env.addSource(inputConsumer13)
                    .flatMap(new OPCJsonDataParserMapFunction())
                    .filter((FilterFunction<PointData>) value -> kafkaStorage13.getPointName().equals(value.getPointName()))
                    .assignTimestampsAndWatermarks(new EventTimeAssigner());
            if (inputParams.get(12).isFill()) {
                inputStream13 = new FillNA(windowStep, kafkaStorage13.getPointName(), heartbeatStream).doOperation(inputStream13)
                        .assignTimestampsAndWatermarks(new EventTimeAssigner());
            }
        }


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
                })
                .join(inputStream3)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream4)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream5)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream6)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream7)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream8)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream9)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream10)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream11)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream12)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream13)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>(first);
                        list.add(second);
                        return list;
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

                        //输入数据
                        Map<String, List<Object>> data = new HashMap<>();
                        List<Object> index = new LinkedList<>();
                        for (InputParam inputParam : inputParams) {
                            data.put(inputParam.getParamName(), new LinkedList<>());
                        }
                        for (List<PointData> row : value) {
                            for (int i = 0; i < inputParams.size(); i++) {
                                data.get(inputParams.get(i).getParamName())
                                        .add(row.get(i).getValue());
                            }
                            index.add(row.get(0).getTimestamp());  //每行数据的时间戳作为dataframe的index
                        }

                        //上次rpc运算结果
                        List<Object> lastRPCResult = lastRPCResultState.value();
                        if (lastRPCResult != null) {
                            for (int i = 0; i < outputParams.size(); i++) {
                                kwargs.put(outputParams.get(i).getParamName(), lastRPCResult.get(i));
                            }
                        }

                        //调用RPC
                        Object[] response = XmlRPCUtils.callRPC(rpcChannel, operatorName, data, index, kwargs);

                        //输出结果
                        if (response.length > 0) {
                            //更新计算结果到状态存储
                            lastRPCResultState.update(Arrays.asList(response));

//                            long timestamp = value.get(0).get(0).getTimestamp();   //这里取每组数据的最小时间戳作为此次rpc计算结果的时间戳
                            long timestamp = value.get(value.size() - 1).get(0).getTimestamp();   //这里取每组数据的最大时间戳作为此次rpc计算结果的时间戳
                            List<PointData> result = new LinkedList<>();
                            for (int i = 0; i < outputParams.size(); i++) {
                                result.add(new PointData(outputParams.get(i).getParamName(),
                                        timestamp,
                                        response[i],
                                        outputParams.get(i).getType()));
                            }
                            return result;
                        }
                        return null;
                    }
                })
                .filter((FilterFunction<List<PointData>>) value -> value != null);


        //存储结果
        DataStream<PointData> resultStream = rpcStream.flatMap(new FlatMapFunction<List<PointData>, PointData>() {
            @Override
            public void flatMap(List<PointData> value, Collector<PointData> out) throws Exception {
                for (PointData p : value) {
                    out.collect(p);
                }
            }
        });
        for (int i = 0; i < outputParams.size(); i++) {
            OutputParam outputParam = outputParams.get(i);
            Map<String, Storage> storageMap = outputParam.getStorage();
            for (String storageType : storageMap.keySet()) {
                if ("influxdb".equalsIgnoreCase(storageType)) {
                    InfluxdbStorage outputInfluxdbStorage = (InfluxdbStorage) storageMap.get(storageType);
                    resultStream.filter((FilterFunction<PointData>) value -> outputParam.getParamName().equals(value.getPointName()))
                            .addSink(new InfluxdbSinker(outputInfluxdbStorage.getUrl(),
                                    outputInfluxdbStorage.getUser(),
                                    outputInfluxdbStorage.getPassword(),
                                    outputInfluxdbStorage.getDatabase(),
                                    outputInfluxdbStorage.getMeasurement(),
                                    tags.get("deviceId"),
                                    jobName));
                } else if ("kafka".equalsIgnoreCase(storageType)) {
                    KafkaStorage outputKafkaStorage = (KafkaStorage) storageMap.get(storageType);

                    Properties producerProperties = new Properties();
                    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, outputKafkaStorage.getBootstrapServers());
                    producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "3000");

                    FlinkKafkaProducer011<String> resultProducer = new FlinkKafkaProducer011<String>(
                            outputKafkaStorage.getTopic(),
                            new SimpleStringSchema(),
                            producerProperties,
                            Optional.empty()  //round-robin to all partitions
                    );
                    resultStream.filter((FilterFunction<PointData>) value -> outputParam.getParamName().equals(value.getPointName()))
                            .map((MapFunction<PointData, String>) value -> {
                                StringBuilder sb = new StringBuilder();
                                sb.append("[");
                                sb.append(value.dump());
                                sb.append("]");
                                return sb.toString();
                            })
                            .addSink(resultProducer);
                }
            }
        }

        //跳变检测逻辑
        DataStream<PointData> hopStream = resultStream
                .keyBy((KeySelector<PointData, String>) value -> value.getPointName())
                .map(new HopJudgeFunction())
                .filter((FilterFunction<PointData>) value -> value != null);

        hopStream.print("HopStream");


        //执行作业
        env.execute(jobName);
    }


    public static class HopJudgeFunction extends RichMapFunction<PointData, PointData> {

        private transient ValueState<Object> lastValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Object> stateDescriptor =
                    new ValueStateDescriptor<>("last value state", Object.class);
            lastValueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public PointData map(PointData value) throws Exception {
            Object lastValue = lastValueState.value();
            PointData output = null;

            if (lastValue != null) {
                if (!value.getValue().equals(lastValue)) {  //发生跳变
                    if (value.getType().equals("double")) {   //对于double类型，返回当前值与前个值的差值
                        output = new PointData(value.getPointName() + "_hopped",
                                value.getTimestamp(),
                                (Double)value.getValue() - (Double) lastValue,
                                "double");
                    } else {  //对于bool类型，从false变为true返回1，从true变为false返回-1
                        output = new PointData(value.getPointName() + "_hopped",
                                value.getTimestamp(),
                                (Boolean)value.getValue() ? 1 : -1,
                                "double");
                    }
                }

            }

            lastValueState.update(value.getValue());
            return output;
        }
    }

}
