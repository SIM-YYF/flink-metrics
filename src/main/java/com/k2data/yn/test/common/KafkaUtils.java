package com.k2data.yn.test.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaUtils {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    /**
     * 向Kafka中发送模拟数据。
     *
     * @param dataFile         测试数据文件，csv类型，第一列为时间戳
     * @param intervalMs       模拟数据发送间隔，单位毫秒
     * @param columns          用于发送模拟数据的csv列名
     * @param roundNum         发送模拟数据的行数，如果文件行数少于count，则从头重新发送
     * @param bootstrapServers kafka链接地址
     * @param topicName        kafka主题名
     */
    public static void generateMockData(String dataFile, long intervalMs, List<String> columns, long roundNum, boolean resetTime, String bootstrapServers, String topicName) throws IOException, InterruptedException {

        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //如果kafka开启了ssl，可以使用如下配置
////        producerProperties.setProperty("ssl.keystore.location", AbnormalProcessing.class.getResource("/kafka_ssl/client.keystore.jks").getPath());
//        producerProperties.setProperty("ssl.keystore.location", Thread.currentThread().getContextClassLoader().getResource("kafka_ssl/client.keystore.jks").getPath());
//        producerProperties.setProperty("ssl.keystore.password", "K2DataKH");
////        producerProperties.setProperty("ssl.truststore.location", AbnormalProcessing.class.getResource("/kafka_ssl/client.truststore.jks").getPath());
//        producerProperties.setProperty("ssl.truststore.location", Thread.currentThread().getContextClassLoader().getResource("kafka_ssl/client.truststore.jks").getPath());
//        producerProperties.setProperty("ssl.truststore.password", "K2DataKH");
//        producerProperties.setProperty("ssl.key.password", "K2DataKH");
//        producerProperties.setProperty("security.protocol", "SSL");

        Producer p = new KafkaProducer(producerProperties);

//        String rootPath = KafkaUtils.class.getResource("/").getPath();
        for (int i = 0; i < roundNum; i++) {
//            BufferedReader br = new BufferedReader(new FileReader(rootPath + File.separator + dataFile));
            BufferedReader br = new BufferedReader(new FileReader(dataFile));
            String line;
            boolean readHeader = false;
            List<String> headers = null;
            DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            while ((line = br.readLine()) != null) {
                if (!readHeader) {
                    String[] items = line.split(",");
                    headers = Arrays.asList(items);
                    readHeader = true;
                    continue;
                }
//                if (num % 2 == 0) {   //模拟数据接入中断
//                    Thread.sleep(intervalMs);
//                    num++;
//                    continue;
//                }
                String[] items = line.split(",");
                long timestamp;
                if (resetTime) {
                    timestamp = System.currentTimeMillis();
                } else {
                    try {
                        timestamp = Long.parseLong(items[0]);  // if the time is Long format
                    } catch (NumberFormatException nfe) {
                        LocalDateTime dateTime = LocalDateTime.from(f.parse(items[0]));  // if the time is yyyy-MM-dd HH:mm:ss format
                        timestamp = Timestamp.valueOf(dateTime).getTime();
                    }
                }

                LOGGER.info("Sending data with timestamp: {}", new Timestamp(timestamp));
                if (headers != null) {
                    JSONArray points = new JSONArray();
                    for (String columnName : columns) {
                        int colIndex = headers.indexOf(columnName);
                        if (colIndex == -1) {
                            LOGGER.error("Not found column in data file: {}", columnName);
                            br.close();
                            p.close();
                            throw new ArrayIndexOutOfBoundsException("Not found column in data file: " + columnName);
                        }
                        points.put(convertDataToJson(timestamp, headers.get(colIndex), Double.parseDouble(items[colIndex])));
                    }
                    p.send(new ProducerRecord(topicName, points.toString()),
                            (metadata, exception) -> {
                                if (exception != null) {
                                    LOGGER.error("Failed to produce kafka record: {}", exception.getMessage());
                                }
                            });
                }

                Thread.sleep(intervalMs);
            }
            br.close();
            Thread.sleep(intervalMs);
        }

        p.close();
    }

    /**
     * 将数据转换为ESF能够处理的json格式。
     *
     * @return
     */
    public static JSONObject convertDataToJson(long time, String pointName, Object pointValue) {
        JSONObject obj = new JSONObject();

        JSONObject tags = new JSONObject();
//        tags.put("device_id", "");
        tags.put("node_id", pointName);
        obj.put("tags", tags);

        JSONObject fields = new JSONObject();
//        fields.put("state", 0);
        fields.put("value", pointValue);
//        fields.put("quality", 192);
        obj.put("fields", fields);

        obj.put("time", time * 1000000);
//        obj.put("measurement", "esf_mock_data");

        return obj;
    }

    /**
     * 处理OPC-sinker采集上来的原始bool类型数据。
     *
     * @param value
     * @return
     */
    public static double parseOPCValue(String value) {
        if (value.startsWith("T") || value.startsWith("t")) {
            return 1;
        }
        if (value.startsWith("F") || value.startsWith("f")) {
            return 0;
        }
        return Double.parseDouble(value);
    }

}
