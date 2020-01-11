package com.k2data.yn.test.flink;

import com.k2data.yn.test.pojo.PointData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class CSVSource implements SourceFunction<PointData> {
    private boolean running = true;
    private String filePath;
    private long intervalMilliSecs;
    private List<String> columns;


    public CSVSource(String filePath, long intervalMilliSecs, List<String> columns) {
        this.filePath = filePath;
        this.intervalMilliSecs = intervalMilliSecs;
        this.columns = columns;
    }

    @Override
    public void run(SourceContext<PointData> sourceContext) throws Exception {
        String rootPath = this.getClass().getResource("/").getPath();
        BufferedReader br = new BufferedReader(new FileReader(rootPath + File.separator + filePath));
        String line;
        boolean readHeader = false;
        List<String> headers = null;
        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        while (running && (line = br.readLine()) != null) {
            if (!readHeader) {
                String[] items = line.split(",");
                headers = Arrays.asList(items);
                readHeader = true;
                continue;
            }
            String[] items = line.split(",", line.length());
            long timestamp = Long.MIN_VALUE;
            try {
                timestamp = Long.parseLong(items[0]);  // if the time is Long format
            } catch (NumberFormatException nfe) {
                LocalDateTime dateTime = LocalDateTime.from(f.parse(items[0]));  // if the time is yyyy-MM-dd HH:mm:ss format
                timestamp = Timestamp.valueOf(dateTime).getTime();
            }

            if (headers != null) {
                for (String columnName : columns) {
                    int colIndex = headers.indexOf(columnName);
                    Object value = null;
                    String type = "double";
                    try {
                        value = Double.parseDouble(items[colIndex]);
                    } catch (NumberFormatException e) {
                        value = Boolean.parseBoolean(items[colIndex]);
                        type = "bool";
                    }
                    sourceContext.collect(new PointData(headers.get(colIndex), timestamp, value, type));
                }
            }

            Thread.sleep(intervalMilliSecs);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
