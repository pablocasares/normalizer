package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.FlatMapperFunction;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class ArrayFlattenMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("array-flatten-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {

        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        ArrayFlattenMapper myFilter = (ArrayFlattenMapper) myFunc;
        assertEquals("ARRAY", myFilter.flatDimension);
    }

    @Test
    public void simpleArrayProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        ArrayFlattenMapper myFilter = (ArrayFlattenMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);
        message.put("ARRAY", Arrays.asList("X", "Y", "Z"));


        List<KeyValue<String, Map<String, Object>>> expectedResult = new ArrayList<>();

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.put("ARRAY", "X");

        expectedResult.add(new KeyValue<>("KEY_1", expected));

        expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.put("ARRAY", "Y");

        expectedResult.add(new KeyValue<>("KEY_1", expected));

        expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.put("ARRAY", "Z");

        expectedResult.add(new KeyValue<>("KEY_1", expected));

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFilter.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void mapArrayProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        ArrayFlattenMapper myFilter = (ArrayFlattenMapper) myFunc;

        Map<String, Object> value1 = new HashMap<>();
        value1.put("type", "type1");
        value1.put("value", "value1");
        Map<String, Object> value2 = new HashMap<>();
        value1.put("type", "type2");
        value1.put("value", "value2");
        Map<String, Object> value3 = new HashMap<>();
        value1.put("type", "type3");
        value1.put("value", "value3");

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);
        message.put("ARRAY", Arrays.asList(value1, value2, value3));


        List<KeyValue<String, Map<String, Object>>> expectedResult = new ArrayList<>();

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.putAll(value1);

        expectedResult.add(new KeyValue<>("KEY_1", expected));

        expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.putAll(value2);

        expectedResult.add(new KeyValue<>("KEY_1", expected));

        expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.putAll(value3);

        expectedResult.add(new KeyValue<>("KEY_1", expected));

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFilter.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void notDimensionProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        ArrayFlattenMapper myFilter = (ArrayFlattenMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFilter.process("KEY_1", message);

        assertEquals(Collections.singletonList(new KeyValue<>("KEY_1", expected)), result);
    }

    @Test
    public void nullArrayProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        ArrayFlattenMapper myFilter = (ArrayFlattenMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);
        message.put("ARRAY", null);

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFilter.process("KEY_1", message);

        assertEquals(Collections.singletonList(new KeyValue<>("KEY_1", expected)), result);
    }

    @Test
    public void processNullKeyAndNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArrayFlattenMapper);
        ArrayFlattenMapper myMapper = (ArrayFlattenMapper) myFunc;

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myMapper.process(null, null);
        for (KeyValue<String, Map<String, Object>> keyValue : result) {
            assertNull(keyValue.key);
            assertNull(keyValue.value);
        }
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myArrayFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArrayFlattenMapper);
        ArrayFlattenMapper myMapper = (ArrayFlattenMapper) myFunc;

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myMapper.process("key", null);
        for (KeyValue<String, Map<String, Object>> keyValue : result) {
            assertEquals("key", keyValue.key);
            assertNull(keyValue.value);
        }
    }

}
