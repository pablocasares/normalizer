package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.mocks.MockProcessContext;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

public class DiffCounterStoreMapperUnitTest {
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);
    private static DiffCounterStoreMapper diffCounterStoreMapper;

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("diff-counter-store-mapper-stream.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);

        streamBuilder.builder(model);
        Map<String, Function> functionsMap = streamBuilder.getFunctions("stream1");
        diffCounterStoreMapper = (DiffCounterStoreMapper) functionsMap.get("diffCounterMapper");
        diffCounterStoreMapper.init(new MockProcessContext());
    }

    @Test
    public void building(){
        assertEquals(true, diffCounterStoreMapper.sendIfZero);
        assertEquals(Collections.singletonList("X"), diffCounterStoreMapper.counterFields);
        assertNotNull(diffCounterStoreMapper.storeCounter);
    }

    @Test
    public void firstTimeViewIsFalse() {
        diffCounterStoreMapper.sendIfZero = true;
        diffCounterStoreMapper.firstTimeView = false;

        String key1 = randomKey();
        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334655L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("X", 0L);
        expected2.put("time", 1122334655L);
        expected2.put("last_timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertNull(result1);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void processSimpleMessage() {
        diffCounterStoreMapper.firstTimeView = true;

        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 2500L);
        message2.put("time", 1122334555L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("X", 1500L);
        expected2.put("time", 1122334555L);
        expected2.put("last_timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void processNullMessage() {
        diffCounterStoreMapper.firstTimeView = true;

        Map<String, Object> message1 = null;

        Map<String, Object> expected1 = null;

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(null, message1);
        assertEquals(null, result1.key);
        assertEquals(expected1, result1.value);
    }

    @Test
    public void sendZeroIfIsEnabled() {
        diffCounterStoreMapper.sendIfZero = true;
        diffCounterStoreMapper.firstTimeView = true;

        String key1 = randomKey();
        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334655L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("X", 0L);
        expected2.put("time", 1122334655L);
        expected2.put("last_timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void NotSendZeroIfIsEnabled() {
        diffCounterStoreMapper.sendIfZero = false;
        diffCounterStoreMapper.firstTimeView = true;

        String key1 = randomKey();
        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334555L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);


        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("time", 1122334555L);
        expected2.put("last_timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void sendDifferentsKeys() {
        diffCounterStoreMapper.firstTimeView = true;

        String key1 = randomKey();
        String key2 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334655L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("time", 1122334655L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key2, message2);
        assertEquals(key2, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void sendWithKeyDefinition() {
        diffCounterStoreMapper.firstTimeView = true;

        String key1 = randomKey();
        String key2 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("gateway", "gateway_1");
        message1.put("interface", "interface_1");
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("gateway", "gateway_1");
        message2.put("interface", "interface_1");
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334655L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("gateway", "gateway_1");
        expected1.put("interface", "interface_1");
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("gateway", "gateway_1");
        expected2.put("interface", "interface_1");
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("X", 0L);
        expected2.put("time", 1122334655L);
        expected2.put("last_timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key2, message2);
        assertEquals(key2, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void processMessageWithNullField() {
        diffCounterStoreMapper.firstTimeView = true;

        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 2500L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("time", 1122334655L);

        Map<String, Object> message3 = new HashMap<>();
        message3.put("A", "VALUE-A");
        message3.put("B", "VALUE-B");
        message3.put("C", 1234567890L);
        message3.put("X", 4000L);
        message3.put("time", 1122334855L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("time", 1122334655L);
        expected2.put("last_timestamp", 1122334455L);

        Map<String, Object> expected3 = new HashMap<>();
        expected3.put("A", "VALUE-A");
        expected3.put("B", "VALUE-B");
        expected3.put("C", 1234567890L);
        expected3.put("X", 1500L);
        expected3.put("time", 1122334855L);
        expected3.put("last_timestamp", 1122334655L);

        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);

        KeyValue<String, Map<String, Object>> result3 = diffCounterStoreMapper.process(key1, message3);
        assertEquals(key1, result3.key);
        assertEquals(expected3, result3.value);
    }

    @Test
    public void processMessageWithSecondNullTimestamp() {
        diffCounterStoreMapper.firstTimeView = true;

        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 2500L);
        message1.put("time", 1122334455L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", 1122334455L);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("last_timestamp", 1122334455L);
        expected2.put("time", System.currentTimeMillis()/1000);


        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void processMessageWithoutTimestamp() {
        diffCounterStoreMapper.firstTimeView = true;

        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 2500L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", System.currentTimeMillis()/1000);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("last_timestamp", System.currentTimeMillis()/1000);
        expected2.put("time", System.currentTimeMillis()/1000);


        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void processMessageWithFirstNullTimestamp() {
        diffCounterStoreMapper.firstTimeView = true;

        String key1 = randomKey();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 2500L);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("time", System.currentTimeMillis()/1000 + 100);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("A", "VALUE-A");
        expected1.put("B", "VALUE-B");
        expected1.put("C", 1234567890L);
        expected1.put("time", System.currentTimeMillis()/1000);

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("A", "VALUE-A");
        expected2.put("B", "VALUE-B");
        expected2.put("C", 1234567890L);
        expected2.put("last_timestamp", System.currentTimeMillis()/1000);
        expected2.put("time", System.currentTimeMillis()/1000 + 100);


        KeyValue<String, Map<String, Object>> result1 = diffCounterStoreMapper.process(key1, message1);
        assertEquals(key1, result1.key);
        assertEquals(expected1, result1.value);

        KeyValue<String, Map<String, Object>> result2 = diffCounterStoreMapper.process(key1, message2);
        assertEquals(key1, result2.key);
        assertEquals(expected2, result2.value);
    }

    @Test
    public void toStringTest(){
        assertNotNull(diffCounterStoreMapper);
        assertEquals(" {counters: [X], sendIfZero: true, stores: mock-key-value-store, timestamp: time, keys: [gateway, interface], firstTimeView: true} ",
                diffCounterStoreMapper.toString());
    }

    @AfterClass
    public static void stopTest(){
        streamBuilder.close();
    }

    private String randomKey(){
        return UUID.randomUUID().toString();
    }
}
