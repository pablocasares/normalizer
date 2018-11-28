package io.wizzie.normalizer.funcs.impl;

import io.wizzie.normalizer.funcs.FilterFunc;
import io.wizzie.normalizer.funcs.FlatMapperFunction;
import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.FilterFunc;
import io.wizzie.normalizer.funcs.FlatMapperFunction;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FormatterFlatMapper extends FlatMapperFunction {
    private static final Logger log = LoggerFactory.getLogger(FormatterFlatMapper.class);

    private enum Type {CONSTANT, FIELDVALUE}

    List<String> commonFields;
    Map<String, FilterFunc> filters = new HashMap<>();
    List<Map<String, Object>> generators;
    Boolean passIfNotApply = false;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        commonFields = (List<String>) properties.getOrDefault("commonFields", new ArrayList<>());
        List<Map<String, Object>> filterMaps = (List<Map<String, Object>>) properties.getOrDefault("filters", new ArrayList<>());
        passIfNotApply = (Boolean) properties.getOrDefault("passIfNotApply", false);

        for (Map<String, Object> filterDefinition : filterMaps) {
            String filterName = (String) filterDefinition.get("name");
            String className = (String) filterDefinition.get("className");

            try {
                Class funcClass = Class.forName(className);
                FilterFunc func = (FilterFunc) funcClass.newInstance();
                func.init((Map<String, Object>) filterDefinition.get("properties"), metricsManager);
                filters.put(filterName, func);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        generators = (List<Map<String, Object>>) properties.getOrDefault("generators", new ArrayList<>());
    }


    @Override
    public Iterable<KeyValue<String, Map<String, Object>>> process(String key, Map<String, Object> value) {
        List<KeyValue<String, Map<String, Object>>> toSend = new ArrayList<>();

        if (value != null) {
            Map<String, Object> base = new HashMap<>();
            commonFields.forEach(field -> {
                if (value.containsKey(field)) base.put(field, value.get(field));
            });

            Boolean someApply = false;

            for (Map<String, Object> generator : generators) {
                String filter = (String) generator.get("filter");

                if (filters.containsKey(filter) && filters.get(filter).test(key, value)) {
                    List<Map<String, Object>> definitions = (List<Map<String, Object>>) generator.get("definitions");
                    someApply = true;

                    for (Map<String, Object> definition : definitions) {
                        List<Map<String, Object>> fields = (List<Map<String, Object>>) definition.get("apply");
                        Map<String, Object> message = new HashMap<>();
                        message.putAll(base);

                        boolean isNullValue = false;

                        for (Map<String, Object> field : fields) {
                            String fieldName = (String) field.get("field");
                            Object contentValue = null;
                            Map<String, Object> content = (Map<String, Object>) field.get("content");
                            Type type = Type.valueOf(content.get("type").toString().toUpperCase());

                            switch (type) {
                                case CONSTANT:
                                    contentValue = content.get("value");
                                    break;
                                case FIELDVALUE:
                                    contentValue = value.get(content.get("value"));
                                    break;
                            }

                            if (contentValue == null) {
                                log.warn("Detected null value for field [{}]", content.get("value"));
                                isNullValue = true;
                                break;
                            }

                            message.put(fieldName, contentValue);
                        }

                        if (!isNullValue)
                            toSend.add(new KeyValue<>(key, message));
                    }
                }
            }

            if (passIfNotApply && !someApply) {
                toSend.add(new KeyValue<>(key, value));
            }
        }

        return toSend;
    }

    @Override
    public void stop() {
    }
}
