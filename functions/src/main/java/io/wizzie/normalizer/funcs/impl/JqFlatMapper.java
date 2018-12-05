package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.FlatMapperFunction;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JqFlatMapper extends FlatMapperFunction {
    private static final Logger log = LoggerFactory.getLogger(JqFlatMapper.class);
    ObjectMapper MAPPER = new ObjectMapper();
    JsonQuery query;
    String jqQuery;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        jqQuery = (String) properties.get("jqQuery");
        if (jqQuery != null) {
            try {
                query = JsonQuery.compile(jqQuery);
            } catch (JsonQueryException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public Iterable<KeyValue<String, Map<String, Object>>> process(String key, Map<String, Object> value) {
        List<KeyValue<String, Map<String, Object>>> result = new ArrayList<>();

        if (value != null) {
            JsonNode node = MAPPER.convertValue(value, JsonNode.class);
            try {
                List<JsonNode> resultQuery = query.apply(node);

                for (JsonNode r : resultQuery) {
                    Map<String, Object> map = MAPPER.convertValue(r, Map.class);
                    result.add(new KeyValue<>(key, map));
                }
            } catch (JsonQueryException e) {
                log.error("Error applying query " + jqQuery + " to message " + value, e);
            }
        }

        return result;
    }

    @Override
    public void stop() { }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(" {")
                .append("jqQuery: ").append(jqQuery)
                .append("} ");

        return builder.toString();
    }
}
