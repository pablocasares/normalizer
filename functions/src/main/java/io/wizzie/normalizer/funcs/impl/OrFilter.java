package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.FilterFunc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OrFilter extends FilterFunc {
    private static final Logger log = LoggerFactory.getLogger(OrFilter.class);
    List<FilterFunc> filters = new ArrayList<>();

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        List<Map<String, Object>> filtersMap = (List<Map<String, Object>>) properties.get("filters");
        for (Map<String, Object> filter : filtersMap) {
            String className = (String) filter.get("className");

            try {
                Class funcClass = Class.forName(className);
                FilterFunc func = (FilterFunc) funcClass.newInstance();
                func.init((Map<String, Object>) filter.get("properties"), metricsManager);
                filters.add(func);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the function {}", className);
            } catch (InstantiationException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the function " + className, e);
            }
        }


    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if(value == null) return false;
        return filters.parallelStream().map(filter -> filter.test(key, value)).reduce((x,y) -> x || y)
                .orElseGet(() -> Boolean.FALSE);
    }

    @Override
    public void stop() {

    }
}
