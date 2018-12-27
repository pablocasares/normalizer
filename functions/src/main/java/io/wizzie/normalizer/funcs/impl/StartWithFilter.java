package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.base.utils.Constants;
import io.wizzie.normalizer.funcs.FilterFunc;

import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class StartWithFilter extends FilterFunc {

    String startWithValue;
    String dimension;
    Boolean isDimensionKey = false;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        startWithValue = checkNotNull((String) properties.get("startWith"), "startWith cannot be null");
        dimension = checkNotNull((String) properties.get("dimension"), "dimension cannot be null");

        if (dimension.equals(Constants.__KEY)) isDimensionKey = true;
    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if (value != null) {
            if (isDimensionKey && key != null)
                return key.startsWith(startWithValue);
            else if (!isDimensionKey) {
                Object currentValue = value.get(dimension);
                return currentValue != null && currentValue.toString().startsWith(startWithValue);
            }
            return false;
        } else {
            return false;
        }
    }

    @Override
    public void stop() {

    }
}
