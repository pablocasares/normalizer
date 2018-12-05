package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.base.utils.Constants;
import io.wizzie.normalizer.funcs.FilterFunc;

import java.util.Map;

public class FieldFilter extends FilterFunc {
    String dimension;
    Object dimensionValue;
    Boolean isDimensionKey = false;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) properties.get("dimension");
        dimensionValue = properties.get("value");

        if (dimension == null || dimension.equals(Constants.__KEY)) isDimensionKey = true;
    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if (value != null) {
            if (isDimensionKey) {
                if (key == null) {
                    return false;
                } else {
                    return key.equals(dimensionValue);
                }
            } else {
                Object currentValue = value.get(dimension);
                return (dimensionValue == null && currentValue == dimensionValue) || (currentValue != null && currentValue.equals(dimensionValue));
            }
        } else {
            return false;
        }
    }

    @Override
    public void stop() {

    }
}
