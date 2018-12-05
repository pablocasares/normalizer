package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleMapper extends MapperFunction {
    List<MapperModel> mappers;
    boolean deleteMode;
    boolean deleteEmpty;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        List<Map<String, Object>> maps = (List<Map<String, Object>>) properties.get("maps");
        mappers = maps.stream()
                .map(map -> new MapperModel((List<String>) map.get("dimPath"), (String) map.get("as")))
                .collect(Collectors.toList());
        deleteMode = (boolean) properties.getOrDefault("deleteMode", false);
        deleteEmpty = (boolean) properties.getOrDefault("deleteEmpty", true);

    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        Map<String, Object> newEvent = new HashMap<>();

        if (value != null && mappers != null) {

            if (deleteMode) {
                newEvent.putAll(value);
            }

            mappers.forEach(mapper -> {
                Integer depth = mapper.dimPath.size() - 1;
                Map<String, Object> levelPath;
                if (deleteMode) {
                    levelPath = newEvent;
                } else {
                    levelPath = new HashMap<>(value);
                }
                List<Map<String, Object>> parents = new LinkedList<>();
                parents.add(newEvent);
                for (Integer level = 0; level < depth; level++) {
                    if (!deleteMode) {
                        if (levelPath != null && levelPath.get(mapper.dimPath.get(level)) instanceof Map) {
                            levelPath = (Map<String, Object>) levelPath.get(mapper.dimPath.get(level));
                        } else {
                            levelPath = null;
                            break;
                        }
                    } else {
                        if (levelPath.get(mapper.dimPath.get(level)) instanceof Map) {
                            levelPath = (Map<String, Object>) levelPath.get(mapper.dimPath.get(level));
                            parents.add(levelPath);
                            if (level == depth - 1) {
                                levelPath.remove(mapper.dimPath.get(depth));
                            }
                        } else {
                            break;
                        }
                    }
                }

                if (depth == 0) {
                    newEvent.remove(mapper.dimPath.get(depth));
                }
                if (deleteEmpty && deleteMode && parents.size() > 1) {
                    for (Integer level = depth - 1; level >= 0; level--) {
                        if (((Map<String, Object>) parents.get(level).get(mapper.dimPath.get(level))).isEmpty()) {
                            if (level == 0) {
                                newEvent.remove(mapper.dimPath.get(level));
                            } else {
                                parents.get(level).remove(mapper.dimPath.get(level));
                            }
                        }
                    }
                }

                if (levelPath != null && !deleteMode) {
                    Object newValue = levelPath.get(mapper.dimPath.get(depth));
                    if (newValue != null) newEvent.put(mapper.as, newValue);
                }
            });

            return new KeyValue<>(key, newEvent);
        } else

        {
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
