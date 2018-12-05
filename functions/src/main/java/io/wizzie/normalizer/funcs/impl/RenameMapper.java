package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RenameMapper extends MapperFunction {
    List<MapperModel> mappers;
    boolean deleteOldField;
    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        List<Map<String, Object>> maps = (List<Map<String, Object>>) properties.get("maps");
        deleteOldField = (boolean) properties.getOrDefault("deleteOldField", true);
        mappers = maps.stream()
                .map(map -> new MapperModel((List<String>) map.get("dimPath"), (String) map.get("as")))
                .collect(Collectors.toList());
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        if (value != null && mappers != null) {
            Map<String, Object> newEvent = new HashMap<>(value);

            mappers.forEach(mapper -> {
                Integer depth = mapper.dimPath.size() - 1;

                Map<String, Object> levelPath = new HashMap<>(value);
                for (Integer level = 0; level < depth; level++) {
                    if (levelPath != null) {
                        levelPath = (Map<String, Object>) levelPath.remove(mapper.dimPath.get(level));
                    }else{
                        break;
                    }
                }

                if (levelPath != null) {
                    Object newValue = levelPath.remove(mapper.dimPath.get(depth));
                    if (newValue != null){
                        newEvent.put(mapper.as, newValue);
                        if(deleteOldField) newEvent.remove(mapper.dimPath.get(0));
                    }
                }
            });

            return new KeyValue<>(key, newEvent);
        } else {
            return new KeyValue<>(key, null);
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        mappers.forEach(mapper -> builder.append(mapper.toString()));
        builder.append("]");

        return builder.toString();
    }


    public class MapperModel {
        List<String> dimPath;
        String as;

        MapperModel(List<String> dimPath,
                    String as) {
            this.dimPath = dimPath;

            if (as != null) {
                this.as = as;
            } else if (dimPath != null) {
                this.as = dimPath.get(dimPath.size() - 1);
            }
        }

        public List<String> getDimPath() {
            return dimPath;
        }

        public String getAs() {
            return as;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(" {")
                    .append("dimPath: ").append(dimPath).append(", ")
                    .append("as: ").append(as)
                    .append("} ");

            return builder.toString();
        }
    }
}
