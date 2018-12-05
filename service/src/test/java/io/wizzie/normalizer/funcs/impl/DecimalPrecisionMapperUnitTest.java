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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DecimalPrecisionMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("decimal-precision-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myDecimalPrecisionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
    }

    @Test
    public void simpleMessageProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myDecimalPrecisionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        DecimalPrecisionMapper myDecimalPrecisionMapper = (DecimalPrecisionMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", Math.PI);
        message.put("B", 1.234);
        message.put("X", 6.99);
        message.put("Y", 1.432);
        message.put("extra-long", Math.E);

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", 3.142);
        expected.put("B", 1.234);
        expected.put("X", 6.99);
        expected.put("Y", 1.43);
        expected.put("extra-long", 2.71828182846);

        KeyValue<String, Map<String, Object>> expectedResult = new KeyValue<>("KEY_1", expected);

        KeyValue<String, Map<String, Object>> result = myDecimalPrecisionMapper.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void nullDimensionProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myDecimalPrecisionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        DecimalPrecisionMapper myDecimalPrecisionMapper = (DecimalPrecisionMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", null);
        message.put("B", 1.234);
        message.put("X", 6.99);
        message.put("Y", 1.432);

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", null);
        expected.put("B", 1.234);
        expected.put("X", 6.99);
        expected.put("Y", 1.43);


        KeyValue<String, Map<String, Object>> expectedResult = new KeyValue<>("KEY_1", expected);
        KeyValue<String, Map<String, Object>> result = myDecimalPrecisionMapper.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void notDimensionProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myDecimalPrecisionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        DecimalPrecisionMapper myDecimalPrecisionMapper = (DecimalPrecisionMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("I", "VALUE-A");
        message.put("J", "VALUE-B");
        message.put("K", 12345);

        Map<String, Object> expected = new HashMap<>();
        expected.put("I", "VALUE-A");
        expected.put("J", "VALUE-B");
        expected.put("K", 12345);

        KeyValue<String, Map<String, Object>> expectedResult = new KeyValue<>("KEY_1", expected);
        KeyValue<String, Map<String, Object>> result = myDecimalPrecisionMapper.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void notNumberDimensionProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myDecimalPrecisionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        DecimalPrecisionMapper myDecimalPrecisionMapper = (DecimalPrecisionMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", null);
        message.put("B", true);
        message.put("X", "test");

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", null);
        expected.put("B", true);
        expected.put("X", "test");


        KeyValue<String, Map<String, Object>> expectedResult = new KeyValue<>("KEY_1", expected);
        KeyValue<String, Map<String, Object>> result = myDecimalPrecisionMapper.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void processNullKeyAndNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myDecimalPrecisionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        DecimalPrecisionMapper myDecimalPrecisionMapper = (DecimalPrecisionMapper) myFunc;

        KeyValue<String, Map<String, Object>> result = myDecimalPrecisionMapper.process(null, null);
        assertNull(result.key);
        assertNull(result.value);
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myDecimalPrecisionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        DecimalPrecisionMapper myDecimalPrecisionMapper = (DecimalPrecisionMapper) myFunc;

        KeyValue<String, Map<String, Object>> result = myDecimalPrecisionMapper.process("key", null);
        assertEquals("key", result.key);
        assertNull(result.value);
    }

}
