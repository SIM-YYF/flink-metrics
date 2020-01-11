package com.k2data.yn.test;

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

public class JobDemo2_6 {

    private final static Logger LOGGER = LoggerFactory.getLogger(JobDemo2_6.class);

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);


        String jobName = "modelId_deviceId_operatorId_1006";
        Map<String, String> tags = new HashMap<>();
        tags.put("deviceId", "device_001");

        String operatorName = "pump_break_diagnosis";

        String windowType = "count";  //count | time
        long windowSize = 1;
        long windowStep = 1;

        //控制参数
        String rpcChannel = "10.1.10.21:5568";

        Map<String, Object> kwargs = new HashMap<>();
        kwargs.put("pump_break_flag", false);
        kwargs.put("pressure_standard", 0.434);
        kwargs.put("flow_standard", 326.5);
        kwargs.put("maybe_error", 0);
        kwargs.put("maybe_repair", 0);

        String inputType = "kafka";
        List<InputParam> inputParams = new LinkedList<>();
        Map<String, Storage> storage1 = new HashMap<>();
        storage1.put(inputType, new KafkaStorage("10.1.10.21:9092", "modelId_deviceId_operatorId_1001_result", "running_healthy_result"));
        inputParams.add(new InputParam("running_healthy_result", "double", 1000, false, storage1));
        Map<String, Storage> storage2 = new HashMap<>();
        storage2.put(inputType, new KafkaStorage("10.1.10.21:9092", "modelId_deviceId_operatorId_1002_result", "pressure_sensor_healthy_result"));
        inputParams.add(new InputParam("pressure_sensor_healthy_result", "double", 1000, false, storage2));
        Map<String, Storage> storage3 = new HashMap<>();
        storage3.put(inputType, new KafkaStorage("10.1.10.21:9092", "modelId_deviceId_operatorId_1003_result", "flow_sensor_healthy_result"));
        inputParams.add(new InputParam("flow_sensor_healthy_result", "double", 1000, false, storage3));
        Map<String, Storage> storage4 = new HashMap<>();
        storage4.put(inputType, new KafkaStorage("10.1.10.21:9092", "modelId_deviceId_operatorId_1004_result", "pressure_decrease_vastly_result"));
        inputParams.add(new InputParam("pressure_decrease_vastly_result", "double", 1000, false, storage4));
        Map<String, Storage> storage5 = new HashMap<>();
        storage5.put(inputType, new KafkaStorage("10.1.10.21:9092", "modelId_deviceId_operatorId_1005_result", "flow_decrease_vastly_result"));
        inputParams.add(new InputParam("flow_decrease_vastly_result", "double", 1000, false, storage5));
        Map<String, Storage> storage6 = new HashMap<>();
        storage6.put(inputType, new KafkaStorage("10.1.10.21:9092", "esf-wenfei", "P17_YL"));
        inputParams.add(new InputParam("pressure", "double", 1000, false, storage6));
        Map<String, Storage> storage7 = new HashMap<>();
        storage7.put(inputType, new KafkaStorage("10.1.10.21:9092", "esf-wenfei", "F12"));
        inputParams.add(new InputParam("flow", "double", 1000, false, storage7));


        List<OutputParam> outputParams = new LinkedList<>();
        Map<String, Storage> outputStorage1 = new HashMap<>();
        outputStorage1.put("influxdb", new InfluxdbStorage("http://10.1.10.21:8086", "k2data", "K2data1234", "esf", "device_001_result", "pump_break_flag"));
        outputParams.add(new OutputParam("pump_break_flag", "bool", outputStorage1));
        Map<String, Storage> outputStorage2 = new HashMap<>();
        outputStorage2.put("influxdb", new InfluxdbStorage("http://10.1.10.21:8086", "k2data", "K2data1234", "esf", "device_001_result", "pressure_standard"));
        outputParams.add(new OutputParam("pressure_standard", "double", outputStorage2));
        Map<String, Storage> outputStorage3 = new HashMap<>();
        outputStorage3.put("influxdb", new InfluxdbStorage("http://10.1.10.21:8086", "k2data", "K2data1234", "esf", "device_001_result", "flow_standard"));
        outputParams.add(new OutputParam("flow_standard", "double", outputStorage3));
        Map<String, Storage> outputStorage4 = new HashMap<>();
        outputStorage4.put("influxdb", new InfluxdbStorage("http://10.1.10.21:8086", "k2data", "K2data1234", "esf", "device_001_result", "maybe_error"));
        outputParams.add(new OutputParam("maybe_error", "double", outputStorage4));
        Map<String, Storage> outputStorage5 = new HashMap<>();
        outputStorage5.put("influxdb", new InfluxdbStorage("http://10.1.10.21:8086", "k2data", "K2data1234", "esf", "device_001_result", "maybe_repair"));
        outputParams.add(new OutputParam("maybe_repair", "double", outputStorage5));

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
        }


//        inputStream1.print(" >>>1 inputstream1");
//        inputStream2.print(" >>>2 inputstream2");
//        inputStream3.print(" >>>3 inputstream3");
//        inputStream4.print(" >>>4 inputstream4");
//        inputStream5.print(" >>>5 inputstream5");
//        inputStream6.print(" >>>6 inputstream6");
//        inputStream7.print(" >>>7 inputstream7");


        //合并输入数据
        DataStream<List<PointData>> joinedInputStream = inputStream1
                .join(inputStream2)
                .where((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<PointData, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(PointData first, PointData second) throws Exception {
                        List<PointData> list = new LinkedList<>();
                        list.add(first);
                        list.add(second);
                        return list;
                    }
                })
                .join(inputStream3)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        first.add(second);
                        return first;
                    }
                })
                .join(inputStream4)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        first.add(second);
                        return first;
                    }
                })
                .join(inputStream5)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        first.add(second);
                        return first;
                    }
                })
                .join(inputStream6)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        first.add(second);
                        return first;
                    }
                })
                .join(inputStream7)
                .where((KeySelector<List<PointData>, Long>) value -> value.get(0).getTimestamp())
                .equalTo((KeySelector<PointData, Long>) value -> value.getTimestamp())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowStep)))
                .apply(new JoinFunction<List<PointData>, PointData, List<PointData>>() {
                    @Override
                    public List<PointData> join(List<PointData> first, PointData second) throws Exception {
                        first.add(second);
                        return first;
                    }
                });

//        joinedInputStream.print(">>>>>>>>>> JoinedStream: ");

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


        //执行作业
        env.execute(jobName);
    }

}
