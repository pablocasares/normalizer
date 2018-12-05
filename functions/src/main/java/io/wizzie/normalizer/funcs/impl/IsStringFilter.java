package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.FilterFunc;

import java.util.Map;

public class IsStringFilter extends FilterFunc {
    String dimension;
    Boolean isDimensionKey = false;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) properties.get("dimension");

        if (dimension == null) isDimensionKey = true;
    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if (value != null) {
            Object currentValue = value.get(dimension);
            return currentValue instanceof String;

        } else {
            return false;
        }
    }

    @Override
    public void stop() {

    }
}
