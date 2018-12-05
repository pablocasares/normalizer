package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.FlatMapperFunction;
import org.apache.kafka.streams.KeyValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitterFlatMapper extends FlatMapperFunction {
    private static final Logger log = LoggerFactory.getLogger(SplitterFlatMapper.class);

    SplitterModel splitter;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        splitter = new SplitterModel(
                (List<String>) properties.get("dimensions"),
                (String) properties.get("timestampDim"),
                (String) properties.get("firstTimestampDim"),
                "60");
    }

    @Override
    public Iterable<KeyValue<String, Map<String, Object>>> process(String key, Map<String, Object> value) {
        if (value != null) {
            List<KeyValue<String, Map<String, Object>>> generatedPackets = new ArrayList<>();

            if (value.containsKey(splitter.getFirstTimestampDim()) && value.containsKey(splitter.getTimestampDim())) {

                DateTime packet_start = new DateTime(Long.parseLong(value.get(splitter.getFirstTimestampDim()).toString()) * 1000, DateTimeZone.UTC);
                DateTime packet_end = new DateTime(Long.parseLong(value.get(splitter.getTimestampDim()).toString()) * 1000, DateTimeZone.UTC);

                DateTime this_start;
                DateTime this_end = packet_start;

                long totalDiff = Seconds.secondsBetween(packet_start, packet_end).getSeconds();

                Map<String, Long> data_map = new HashMap<>();
                Map<String, Long> data_count = new HashMap<>();
                for (String dimension : splitter.getDimensions()) {

                    try {
                        if (value.containsKey(dimension)) {
                            data_map.put(dimension, Long.parseLong(value.get(dimension).toString()));
                            data_count.put(dimension, 0L);
                        }
                    } catch (NumberFormatException e) {
                        log.warn(e.getMessage(), e);
                        return generatedPackets;
                    }

                }

                do {
                    long diff, this_data;

                    this_start = this_end;
                    this_end = this_start.plusSeconds(splitter.getInterval() - this_start.getSecondOfMinute());
                    if (this_end.isAfter(packet_end)) this_end = packet_end;
                    diff = Seconds.secondsBetween(this_start, this_end).getSeconds();

                    Map<String, Object> to_send = new HashMap<>();
                    to_send.putAll(value);
                    to_send.put(splitter.getTimestampDim(), this_start.getMillis() / 1000);
                    to_send.remove(splitter.getFirstTimestampDim());

                    for (Map.Entry<String, Long> entry : data_map.entrySet()) {
                        if (totalDiff == 0) this_data = entry.getValue();
                        else this_data = (long) Math.ceil(entry.getValue() * diff / totalDiff);

                        Long data_dimension = data_count.get(entry.getKey());
                        data_dimension += this_data;
                        data_count.put(entry.getKey(), data_dimension);

                        to_send.put(entry.getKey(), this_data);
                    }

                    generatedPackets.add(new KeyValue<>(key, to_send));

                } while (this_end.isBefore(packet_end));

                for (Map.Entry<String, Long> data : data_map.entrySet()) {
                    if (data.getValue() != data_count.get(data.getKey())) {
                        int last_index = generatedPackets.size() - 1;
                        KeyValue<String, Map<String, Object>> last = generatedPackets.get(last_index);
                        long new_data = ((long) last.value.get(data.getKey()) + (data.getValue() - data_count.get(data.getKey())));

                        if (new_data > 0) last.value.put(data.getKey(), new_data);
                    }
                }

            } else if (value.containsKey(splitter.getTimestampDim())) {
                try {
                    for (String dimension : splitter.getDimensions()) {
                        if (value.containsKey(dimension)) {
                            Long data = Long.parseLong(value.get(dimension).toString());
                            value.put(dimension, data);
                        }
                    }

                    generatedPackets.add(new KeyValue<>(key, value));

                } catch (NumberFormatException e) {
                    log.warn(e.getMessage(), e);
                    return generatedPackets;
                }
            } else {

                try {
                    for (String dimension : splitter.getDimensions()) {
                        if (value.containsKey(dimension)) {
                            Long data = Long.parseLong(value.get(dimension).toString());
                            value.put(dimension, data);
                        }
                    }
                    generatedPackets.add(new KeyValue<>(key, value));
                } catch (NumberFormatException e) {
                    log.warn(e.getMessage(), e);
                    return generatedPackets;
                }
            }


            return generatedPackets;
        } else {
            return null;
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();

        builder.append("[");
        builder.append(splitter.toString());
        builder.append("]");

        return builder.toString();
    }

    public class SplitterModel {
        List<String> dimensions;
        Integer interval;
        String firstTimestampDim = "last_timestamp";
        String timestampDim = "timestamp";

        SplitterModel(List<String> dimensions, String timestamp, String firstTimestamp, String interval) {
            this.dimensions = dimensions;
            if (firstTimestamp != null) this.firstTimestampDim = firstTimestamp;
            if (timestamp != null) this.timestampDim = timestamp;
            this.interval = interval != null ? Integer.valueOf(interval) : 60;
        }

        public List<String> getDimensions() {
            return dimensions;
        }

        public Integer getInterval() {
            return interval;
        }

        public String getFirstTimestampDim() {
            return firstTimestampDim;
        }

        public String getTimestampDim() {
            return timestampDim;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(" {")
                    .append("dimensions: ").append(dimensions).append(", ")
                    .append("timestampDim: ").append(timestampDim).append(", ")
                    .append("firstTimestampDim: ").append(firstTimestampDim).append(", ")
                    .append("interval: ").append(interval)
                    .append("} ");

            return builder.toString();
        }
    }

}
