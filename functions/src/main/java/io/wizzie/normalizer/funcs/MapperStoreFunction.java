package io.wizzie.normalizer.funcs;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.base.utils.ConversionUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.wizzie.normalizer.base.utils.Constants.*;

/**
 * This class is used to do a mapper process, but it offers a key value store to operate with multiples states
 */
public abstract class MapperStoreFunction implements Function<KeyValue<String, Map<String, Object>>>,
        Transformer<String, Map<String, Object>, KeyValue<String, Map<String, Object>>> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Map<String, Object> properties;
    private Map<String, KeyValueStore> stores = new HashMap<>();
    private List<String> availableStores;
    private String appId;
    private Long windownTimeMs;
    private MetricsManager metricsManager;

    /**
     * Initialize mapper store function
     * @param properties Properties for mapper store function
     * @param metricsManager MetricsManager object for mapper store function
     */
    @Override
    public void init(Map<String, Object> properties, MetricsManager metricsManager) {
        this.availableStores = (List<String>) properties.get(__STORES);
        this.appId = (String) properties.get(__APP_ID);
        this.windownTimeMs = ConversionUtils.toLong(properties.get(__WINDOW_TIME_MS));
        this.properties = properties;
        this.metricsManager = metricsManager;
    }

    /**
     * Inialize mapper store function
     * @param context The context of processor for mapper store function
     */
    @Override
    public void init(ProcessorContext context) {
        if(windownTimeMs != null) context.schedule(windownTimeMs, PunctuationType.STREAM_TIME, this::window);
        availableStores.forEach((storeName) ->
                stores.put(storeName, (KeyValueStore) context.getStateStore(String.format("%s_%s", appId, storeName)))
        );
        prepare(properties, metricsManager);
        log.info("   with {}", toString());
    }

    /**
     * Process a Key-Value message of Kafka
     * @param key The key of Kafka message
     * @param value The value of Kafka message
     * @return A Key-Value message after process
     */
    @Override
    public KeyValue<String, Map<String, Object>> transform(String key, Map<String, Object> value) {
        return process(key, value);
    }

    /**
     * Close all opened stores and stop process
     */
    @Override
    public void close() {
        stores.values().forEach(StateStore::close);
        stop();
    }

    /**
     * Allow to get a list of available stores
     * @return List of available stores
     */
    public List<String> getAvailableStores(){
        return availableStores;
    }

    /**
     * Allow to get a KeyValue store of its name
     * @param storeName Name of Key-Value store
     * @param <V> Value Type
     * @return A Key-Value store
     */
    public <V> KeyValueStore<String, V> getStore(String storeName){
        return (KeyValueStore<String, V>) stores.get(storeName);
    }

    /**
     * This method allow implement window calls
     * @param timestamp where the window must be called
     * @return A messages that are added to the current stream
     */
    public abstract KeyValue<String, Map<String, Object>> window(long timestamp);
}
