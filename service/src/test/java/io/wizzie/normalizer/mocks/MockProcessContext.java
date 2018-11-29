package io.wizzie.normalizer.mocks;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.*;

import java.io.File;
import java.time.Duration;
import java.util.Map;

public class MockProcessContext implements ProcessorContext {
    @Override
    public String applicationId() {
        return null;
    }

    @Override
    public TaskId taskId() {
        return null;
    }

    @Override
    public Serde<?> keySerde() {
        return null;
    }

    @Override
    public Serde<?> valueSerde() {
        return null;
    }

    @Override
    public Cancellable schedule(long interval, PunctuationType type, Punctuator callback) {
        return null;
    }

    @Override
    public Cancellable schedule(Duration duration, PunctuationType punctuationType, Punctuator punctuator) throws IllegalArgumentException {
        return null;
    }

    @Override
    public File stateDir() {
        return null;
    }

    @Override
    public StreamsMetrics metrics() {
        return null;
    }

    @Override
    public void register(StateStore stateStore, StateRestoreCallback stateRestoreCallback) {

    }

    @Override
    public StateStore getStateStore(String name) {
        return new MockKeyValueStore<>();
    }

    @Override
    public <K, V> void forward(K key, V value) {

    }

    @Override
    public <K, V> void forward(K k, V v, To to) {

    }

    @Override
    public <K, V> void forward(K key, V value, int childIndex) {

    }

    @Override
    public <K, V> void forward(K key, V value, String childName) {

    }

    @Override
    public void commit() {

    }

    @Override
    public String topic() {
        return null;
    }

    @Override
    public int partition() {
        return 0;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public Headers headers() {
        return null;
    }

    @Override
    public long timestamp() {
        return 0;
    }

    @Override
    public Map<String, Object> appConfigs() {
        return null;
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(String s) {
        return null;
    }
}
