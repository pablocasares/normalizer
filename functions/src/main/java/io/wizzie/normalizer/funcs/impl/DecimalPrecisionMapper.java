package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.*;

public class DecimalPrecisionMapper extends MapperFunction {
    private static final Logger log = LoggerFactory.getLogger(ArithmeticMapper.class);

    private final static String DIMENSIONS = "dimensions";
    private final static String NUMBER_OF_DECIMALS = "numberOfDecimals";
    private final static String SCALES = "scales";

    private List<Map<String, Object>> scales;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        scales = (List<Map<String, Object>>) properties.getOrDefault(SCALES, new HashMap<>());

        for (Map<String, Object> scale : scales) {
            if (!scale.keySet().contains(DIMENSIONS)) {
                log.warn("Ignored element {} due to bad format map", scales.indexOf(scale));
                scales.remove(scale);
                return;
            }
        }
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> message) {

        if (message != null) {
            for (Map<String, Object> scale : scales) {
                List<String> dimensions = (List<String>) scale.get(DIMENSIONS);
                Integer numberOfDecimals = (Integer) scale.getOrDefault(NUMBER_OF_DECIMALS, 2);

                for (String dimension : dimensions) {
                    if (dimension != null && message.containsKey(dimension)) {
                        Object value = message.get(dimension);
                        if (value != null && value instanceof Number) {
                            Number numberValue = (Number) value;
                            Double newValue  = BigDecimal.valueOf(numberValue.doubleValue()).setScale(numberOfDecimals, RoundingMode.HALF_UP).doubleValue();
                            message.put(dimension, newValue);
                        } else {
                            log.warn("Ignored dimension <{}> with value <{}> due to it isn't a number", dimension, value);
                        }
                    }
                }
            }
        }

        return new KeyValue<>(key, message);
    }

    @Override
    public void stop() {

    }
}
