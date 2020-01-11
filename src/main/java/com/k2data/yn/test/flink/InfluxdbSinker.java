package com.k2data.yn.test.flink;

import com.k2data.yn.test.pojo.PointData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class InfluxdbSinker extends RichSinkFunction<PointData> {
    private final static Logger LOGGER = LoggerFactory.getLogger(InfluxdbSinker.class);

    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private String dbName;
    private String measurement;
    private String deviceId;
    private String jobName;
    private InfluxDB influxDB;

    public InfluxdbSinker(String dbUrl, String dbUser, String dbPassword, String dbName, String measurement, String deviceId, String jobName) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.dbName = dbName;
        this.measurement = measurement;
        this.deviceId = deviceId;
        this.jobName = jobName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.influxDB = InfluxDBFactory.connect(dbUrl, dbUser, dbPassword);
        this.influxDB.setDatabase(this.dbName);
//        this.influxDB.enableBatch(BatchOptions.DEFAULTS);

        influxDB.enableBatch(BatchOptions.DEFAULTS.exceptionHandler(
                new BiConsumer<Iterable<Point>, Throwable>() {
                    @Override
                    public void accept(Iterable<Point> failedPoints, Throwable throwable) {
                        LOGGER.error(throwable.getMessage());
                    }
                })
        );
    }

    @Override
    public void close() throws Exception {
        this.influxDB.close();
        super.close();
    }

    @Override
    public void invoke(PointData value, Context context) throws Exception {
        if (value == null) {
            return;
        }

        if ("double".equalsIgnoreCase(value.getType())) {
            double v = Double.parseDouble(value.getValue().toString());
            influxDB.write(Point.measurement(measurement)
                    .time(value.getTimestamp(), TimeUnit.MILLISECONDS)
                    .tag("device_id", deviceId)
                    .tag("job_name", jobName)
                    .addField(value.getPointName(), Double.parseDouble(value.getValue().toString()))
                    .build());
        } else if ("bool".equalsIgnoreCase(value.getType())) {
            influxDB.write(Point.measurement(measurement)
                    .time(value.getTimestamp(), TimeUnit.MILLISECONDS)
                    .tag("device_id", deviceId)
                    .tag("job_name", jobName)
                    .addField(value.getPointName(), Boolean.parseBoolean(value.getValue().toString()))
                    .build());
        }

    }
}
