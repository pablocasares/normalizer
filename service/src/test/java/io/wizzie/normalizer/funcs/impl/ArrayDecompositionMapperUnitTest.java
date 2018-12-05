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
import java.util.Map;

import static org.junit.Assert.*;

public class ArrayDecompositionMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("array-decomposition-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {

        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("arrayDecompositionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        ArrayDecompositionMapper myFilter = (ArrayDecompositionMapper) myFunc;
        assertEquals("array-dim", myFilter.dimension);
        assertEquals(true, myFilter.deleteDimension);
    }

    @Test
    public void simpleArrayProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("arrayDecompositionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        ArrayDecompositionMapper arrayDecompositionMapper = (ArrayDecompositionMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);
        message.put("array-dim", Arrays.asList("X", "Y", "Z"));


        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.put("dim1", "X");
        expected.put("dim2", "Y");
        expected.put("dim3", "Z");

        KeyValue<String, Map<String, Object>> expectedResult = new KeyValue<>("KEY_1", expected);

        KeyValue<String, Map<String, Object>> result = arrayDecompositionMapper.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void lessDimThanArrayProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("arrayDecompositionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        ArrayDecompositionMapper arrayDecompositionMapper = (ArrayDecompositionMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);
        message.put("array-dim", Arrays.asList("X", "Y", "Z", "W"));


        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.put("dim1", "X");
        expected.put("dim2", "Y");
        expected.put("dim3", "Z");

        KeyValue<String, Map<String, Object>> expectedResult = new KeyValue<>("KEY_1", expected);

        KeyValue<String, Map<String, Object>> result = arrayDecompositionMapper.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void moreDimThanArrayProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("arrayDecompositionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        ArrayDecompositionMapper arrayDecompositionMapper = (ArrayDecompositionMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);
        message.put("array-dim", Arrays.asList("X", "Y"));


        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);
        expected.put("dim1", "X");
        expected.put("dim2", "Y");

        KeyValue<String, Map<String, Object>> expectedResult = new KeyValue<>("KEY_1", expected);

        KeyValue<String, Map<String, Object>> result = arrayDecompositionMapper.process("KEY_1", message);

        assertEquals(expectedResult, result);
    }

    @Test
    public void notDimensionProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("arrayDecompositionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArrayDecompositionMapper);
        ArrayDecompositionMapper arrayDecompositionMapper = (ArrayDecompositionMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);

        KeyValue<String, Map<String, Object>> result = arrayDecompositionMapper.process("KEY_1", message);

        assertEquals(new KeyValue<>("KEY_1", expected), result);
    }

    @Test
    public void nullArrayProcess() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("arrayDecompositionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArrayDecompositionMapper);
        ArrayDecompositionMapper myFilter = (ArrayDecompositionMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");
        message.put("C", 12345);
        message.put("array-dim", null);

        Map<String, Object> expected = new HashMap<>();
        expected.put("A", "VALUE-A");
        expected.put("B", "VALUE-B");
        expected.put("C", 12345);

        KeyValue<String, Map<String, Object>> result = myFilter.process("KEY_1", message);

        assertEquals(new KeyValue<>("KEY_1", expected), result);
    }

    @Test
    public void processNullKeyAndNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("arrayDecompositionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArrayDecompositionMapper);
        ArrayDecompositionMapper myMapper = (ArrayDecompositionMapper) myFunc;


        KeyValue<String, Map<String, Object>> result = myMapper.process(null, null);
        assertNull(result.key);
        assertNull(result.value);
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("arrayDecompositionMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArrayDecompositionMapper);
        ArrayDecompositionMapper myMapper = (ArrayDecompositionMapper) myFunc;

        KeyValue<String, Map<String, Object>> result = myMapper.process("key", null);
        assertEquals("key", result.key);
        assertNull(result.value);
    }

}
