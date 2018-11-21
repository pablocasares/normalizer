package io.wizzie.normalizer.funcs;

import io.wizzie.normalizer.base.utils.Constants;
import io.wizzie.metrics.MetricsManager;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This class is used to implement filters, to keep out message from the streams. You can use this function
 * on the sinks to filter specific messages to other stream or to send to kafka topic
 */
public abstract class FilterFunc implements Function<Boolean>, Predicate<String, Map<String, Object>> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Boolean __MATCH = true;

    /**
     * Initialize filter function
     *
     * @param properties     Properties for filter function
     * @param metricsManager MetricsManager to register custom metrics.
     */
    @Override
    public void init(Map<String, Object> properties, MetricsManager metricsManager) {
        __MATCH = properties.containsKey(Constants.__MATCH) ? (Boolean) properties.get(Constants.__MATCH) : true;
        prepare(properties, metricsManager);
        log.info("   with {}", toString());
    }

    /**
     * Filter a Key-Value Kafka message
     *
     * @param key   The key of Kafka message
     * @param value The value of kafka message
     * @return True if filter match else return false.
     */
    @Override
    public boolean test(String key, Map<String, Object> value) {
        return process(key, value) == __MATCH;
    }

}
