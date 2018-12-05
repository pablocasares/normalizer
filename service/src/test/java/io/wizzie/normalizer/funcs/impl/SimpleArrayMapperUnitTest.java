package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SimpleArrayMapperUnitTest {
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("simple-array-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleArrayMapper);
        SimpleArrayMapper myArrayMapper = (SimpleArrayMapper) myFunc;

        assertEquals("dimArray", myArrayMapper.dimension);
        assertTrue(myArrayMapper.deleteDimension);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleArrayMapper);
        SimpleArrayMapper myArrayMapper = (SimpleArrayMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        List<Object> myList = Arrays.asList("123", "A", 2);
        message.put("dimArray", myList);

        KeyValue<String, Map<String, Object>> mapMessage = myArrayMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("123", value.get("a"));
        assertEquals("A", value.get("b"));
        assertEquals(2, value.get("c"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processMaxIndexMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleArrayMapper);
        SimpleArrayMapper myArrayMapper = (SimpleArrayMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        List<Object> myList = Arrays.asList("123", "A");
        message.put("dimArray", myList);

        KeyValue<String, Map<String, Object>> mapMessage = myArrayMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("123", value.get("a"));
        assertEquals("A", value.get("b"));
        assertFalse(value.containsKey("c"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processNullArrayMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleArrayMapper);
        SimpleArrayMapper myArrayMapper = (SimpleArrayMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        List<Object> myList = Arrays.asList("123", "A");
        message.put("dimArrayNull", myList);

        KeyValue<String, Map<String, Object>> mapMessage = myArrayMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;

        assertEquals(message, value);
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleArrayMapper);
        SimpleArrayMapper myArrayMapper = (SimpleArrayMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        List<Object> myList = Arrays.asList("123", "A", 2);
        message.put("dimArray", myList);

        KeyValue<String, Map<String, Object>> mapMessage = myArrayMapper.process(null, message);
        assertNull(mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("123", value.get("a"));
        assertEquals("A", value.get("b"));
        assertEquals(2, value.get("c"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processNullKeyAndNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleArrayMapper);
        SimpleArrayMapper myArrayMapper = (SimpleArrayMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myArrayMapper.process(null, null);
        assertNull(mapMessage.key);
        assertNull(mapMessage.value);
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleArrayMapper);
        SimpleArrayMapper myArrayMapper = (SimpleArrayMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myArrayMapper.process("KEY", null);
        assertEquals("KEY", mapMessage.key);
        assertNull(mapMessage.value);
    }

    @Test
    public void toStringTest() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayMapper");

        assertNotNull(myFunc);
        assertEquals("{dimension: dimArray, dimensionToIndex: {a=0, b=1, c=2}, deleteDimension: true}"
                , myFunc.toString());
    }

    @AfterClass
    public static void stopTest() {
        streamBuilder.close();
    }
}
