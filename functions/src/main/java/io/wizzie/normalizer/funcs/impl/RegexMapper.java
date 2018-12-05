package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class RegexMapper extends MapperFunction {

    public static final String GENERATE_DIMENSIONS = "generateDimensions";
    public static final String REGEX_PATTERN = "regexPattern";
    public static final String PARSE_DIMENSION = "parseDimension";

    private final String ERROR_MESSAGE_PATTERN = "%s cannot be null";

    List<String> generateDimensions;
    Pattern pattern;
    String parseDimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        String regexPattern = (String) checkNotNull(properties.get(REGEX_PATTERN), String.format(ERROR_MESSAGE_PATTERN, REGEX_PATTERN));
        generateDimensions = (List<String>) checkNotNull(properties.get(GENERATE_DIMENSIONS), String.format(ERROR_MESSAGE_PATTERN, GENERATE_DIMENSIONS));
        parseDimension = (String) checkNotNull(properties.get(PARSE_DIMENSION), String.format(ERROR_MESSAGE_PATTERN, PARSE_DIMENSION));

        pattern = Pattern.compile(regexPattern);
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        Map<String, Object> newValue = null;

        if (value != null) {
            newValue = new HashMap<>(value);
            Object dimension = newValue.get(parseDimension);

            if (dimension != null) {
                Matcher matcher = pattern.matcher(dimension.toString());

                while (matcher.find()) {
                    for (String generateDimension : generateDimensions) {
                        newValue.put(generateDimension, matcher.group(generateDimension));
                    }
                }
            }
        }

        return new KeyValue<>(key, newValue);
    }

    @Override
    public void stop() {
        // Nothing to do
    }
}
