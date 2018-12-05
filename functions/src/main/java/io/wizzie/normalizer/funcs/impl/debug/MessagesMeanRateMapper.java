package io.wizzie.normalizer.funcs.impl.debug;

import com.codahale.metrics.Meter;
import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MessagesMeanRateMapper extends MapperFunction {

    Logger log = LoggerFactory.getLogger(MessagesMeanRateMapper.class);

    Meter messages;
    int forEach;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        messages = new Meter();
        forEach = (int) properties.getOrDefault("print_foreach", 100000);
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        messages.mark();

        if (messages.getCount() % forEach == 0)
            log.info(String.format("Messages rate mean: %.2f (Total: %d)", messages.getMeanRate(), messages.getCount()));

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {

    }
}
