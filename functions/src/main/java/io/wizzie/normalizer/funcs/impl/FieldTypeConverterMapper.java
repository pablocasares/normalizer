package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.base.utils.ConvertFrom;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class FieldTypeConverterMapper extends MapperFunction {

    List<Map<String, Object>> conversions;
    private static final Logger log = LoggerFactory.getLogger(FieldTypeConverterMapper.class);

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        conversions = (List<Map<String, Object>>) properties.get("conversions");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> message) {
        if (message != null) {
            for (Map<String, Object> conversion : conversions) {
                String dimension = checkNotNull((String) conversion.get("dimension"), "dimension cannot be null!");
                String newDimension = (String) conversion.get("newDimension");
                // Get value
                Object value = message.get(dimension);

                // Check that value is not null
                if (value != null) {
                    // Source
                    String from = checkNotNull((String) conversion.get("from"), "from cannot be null!");
                    // Destiny
                    String to = checkNotNull((String) conversion.get("to"), "to cannot be null!");
                    // Converted value
                    Object convertedValue = null;

                    // Performance conversion
                    switch (ConvertFrom.valueOf(to.toUpperCase())) {
                        case NUMBER:
                            try {
                                convertedValue = ConvertFrom.valueOf(from.toUpperCase()).toNumber(value);
                            } catch (ParseException e) {
                                log.debug("ParseException at ConvertFrom function.");
                            }
                            if (convertedValue == null) {
                                log.debug("Value is null or cannot be converted");
                            }
                            break;
                        case STRING:
                            convertedValue = ConvertFrom.valueOf(from.toUpperCase()).toString(value);
                            if (convertedValue == null) {
                                log.debug("Value is null or cannot be converted");
                            }
                            break;
                        case BOOLEAN:
                            convertedValue = ConvertFrom.valueOf(from.toUpperCase()).toBoolean(value);
                            if (convertedValue == null) {
                                log.debug("Value is null or cannot be converted");
                            }
                            break;
                    }

                    // Add converted value to map
                    if (newDimension != null)
                        message.put(newDimension, convertedValue);
                    else
                        message.put(dimension, convertedValue);
                }
            }

            return new KeyValue<>(key, message);
        } else{
            return new KeyValue<>(key, null);
        }
    }

    @Override
    public void stop() {
    }
}
