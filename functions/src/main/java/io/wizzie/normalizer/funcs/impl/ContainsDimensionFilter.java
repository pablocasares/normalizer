package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.FilterFunc;

import java.util.List;
import java.util.Map;

public class ContainsDimensionFilter extends FilterFunc {
    List<String> dimensions;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimensions = (List<String>) properties.get("dimensions");
    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if(value == null) return false;
        else return dimensions.stream().map(value::containsKey).reduce((x,y) -> x && y).orElseGet(() -> Boolean.FALSE);
    }

    @Override
    public void stop() {

    }
}
