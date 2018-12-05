package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.funcs.MapperFunction;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class MaxValueMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("max-value-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMaxValueMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MaxValueMapper myMaxValueFunction = (MaxValueMapper) myFunc;

        assertEquals(myMaxValueFunction.dimension, "measures");
        assertEquals(myMaxValueFunction.newDimension, "max_measure");
    }

    @Test
    public void getMaxDoubleValue() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMaxValueMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MaxValueMapper myMaxValueFunction = (MaxValueMapper) myFunc;

        assertEquals("measures", myMaxValueFunction.dimension);

        List<Double> measures = Arrays.asList(2.5, 1.0, 0.12, 8.56, 4.56, 3.99);

        Map<String, Object> msg = new HashMap<>();
        msg.put("timestamp", 1234567890);
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 2);
        msg.put("measures", measures);

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("max_measure", 8.56);

        assertEquals(new KeyValue<>("KEY-A", expectedMsg), myMaxValueFunction.process("KEY-A", msg));
    }

    @Test
    public void getMaxIntegerValue() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMaxValueMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MaxValueMapper myMaxValueFunction = (MaxValueMapper) myFunc;

        assertEquals("measures", myMaxValueFunction.dimension);

        List<Integer> measures = Arrays.asList(2, 1, 0, 8, 4, 3);

        Map<String, Object> msg = new HashMap<>();
        msg.put("timestamp", 1234567890);
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 2);
        msg.put("measures", measures);

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("max_measure", 8);

        assertEquals(new KeyValue<>("KEY-A", expectedMsg), myMaxValueFunction.process("KEY-A", msg));
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMaxValueMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MaxValueMapper myMaxValueFunction = (MaxValueMapper) myFunc;

        assertEquals("measures", myMaxValueFunction.dimension);

        List<Integer> measures = Arrays.asList(2, 1, 0, 8, 4, 3);

        Map<String, Object> msg = new HashMap<>();
        msg.put("timestamp", 1234567890);
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 2);
        msg.put("measures", measures);

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("max_measure", 8);

        assertEquals(new KeyValue<>(null, expectedMsg), myMaxValueFunction.process(null, msg));
    }

    @Test
    public void processNullMessages() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMaxValueMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MaxValueMapper myMaxValueFunction = (MaxValueMapper) myFunc;

        assertEquals("measures", myMaxValueFunction.dimension);

        assertEquals(new KeyValue<>("KEY-A", null), myMaxValueFunction.process("KEY-A", null));
    }

    @Test
    public void processNullKeysAndMessages() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMaxValueMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MaxValueMapper myMaxValueFunction = (MaxValueMapper) myFunc;

        assertEquals("measures", myMaxValueFunction.dimension);

        assertEquals(new KeyValue<>(null, null), myMaxValueFunction.process(null, null));
    }

    @Test
    public void processNullDimensionMessages() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMaxValueMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MaxValueMapper myMaxValueFunction = (MaxValueMapper) myFunc;

        assertEquals("measures", myMaxValueFunction.dimension);

        Map<String, Object> msg = new HashMap<>();
        msg.put("timestamp", 1234567890);
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 2);

        assertEquals(new KeyValue<>("KEY-A", msg), myMaxValueFunction.process("KEY-A", msg));
    }

}
