package io.wizzie.normalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.wizzie.normalizer.base.utils.Constants;

public class SinkModel {
    public final static String PARTITION_BY_KEY = Constants.__KEY;
    public final static String KAFKA_TYPE = "kafka";
    public final static String STREAM_TYPE = "stream";

    String topic;
    String type;
    String partitionBy;
    FunctionModel filter;

    @JsonCreator
    public SinkModel(@JsonProperty("topic") String topic,
                     @JsonProperty("type") String type,
                     @JsonProperty("partitionBy") String partitionBy,
                     @JsonProperty("filter") FunctionModel filter) {
        this.topic = topic;

        if (partitionBy == null) {
            this.partitionBy = PARTITION_BY_KEY;
        } else {
            this.partitionBy = partitionBy;
        }

        if(type == null) {
            this.type = KAFKA_TYPE;
        } else {
            this.type = type;
        }

        this.filter = filter;
    }

    @JsonProperty
    public String getTopic() {
        return topic;
    }

    @JsonProperty
    public String getPartitionBy() {
        return partitionBy;
    }

    @JsonProperty
    public String getType() {
        return type;
    }

    @JsonProperty
    public FunctionModel getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("topic: ").append(topic).append(", ")
                .append("type: ").append(type).append(", ")
                .append("partitionBy: ").append(partitionBy)
                .append("}");

        return builder.toString();
    }
}
