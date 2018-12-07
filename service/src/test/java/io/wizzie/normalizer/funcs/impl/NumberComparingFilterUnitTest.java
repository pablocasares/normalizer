package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.FunctionException;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.exceptions.TryToDoLoopException;
import io.wizzie.normalizer.funcs.FilterFunc;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


public class NumberComparingFilterUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("number-comparing-filter.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");

        for (Function func : functions.values()) {
            assertNotNull(func);
            assertTrue(func instanceof FilterFunc);
        }
    }

    @Test
    public void processMessageWithGreaterThanNumberFilter() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("greaterThanNumberFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        NumberComparingFilter myFilter = (NumberComparingFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("ValueToCompare", 3);

        assertTrue(myFilter.process("key1", message));

        message.put("ValueToCompare", 1);

        assertFalse(myFilter.process("key1", message));
    }

    @Test
    public void processMessageWithLowerThanNumberFilter() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("lowerThanNumberFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        NumberComparingFilter myFilter = (NumberComparingFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("ValueToCompare", 5.672);

        assertFalse(myFilter.process("key1", message));

        message.put("ValueToCompare", 1.1);

        assertTrue(myFilter.process("key1", message));
    }

    @Test
    public void processMessageWithGreaterThanDimensionFilter() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("greaterThanDimensionFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        NumberComparingFilter myFilter = (NumberComparingFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("ValueA", 8.178);
        message.put("ValueB", 1.32);

        assertTrue(myFilter.process("key1", message));

        message.put("ValueA", 0.5);
        message.put("ValueB", 2.7);

        assertFalse(myFilter.process("key1", message));
    }

    @Test
    public void processMessageWithLowerThanDimensionFilter() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("lowerThanDimensionFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        NumberComparingFilter myFilter = (NumberComparingFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("ValueA", 6.1);
        message.put("ValueB", 1.9);

        assertFalse(myFilter.process("key1", message));

        message.put("ValueA", 0.5);
        message.put("ValueB", 2.7);

        assertTrue(myFilter.process("key1", message));
    }

    @Test
    public void processMessageWithDefaultConditionFilter() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("defaultConditionFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        NumberComparingFilter myFilter = (NumberComparingFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("ValueToCompare", 3.2);

        assertTrue(myFilter.process("key1", message));

        message.put("ValueToCompare", 1.3);

        assertFalse(myFilter.process("key1", message));
    }

    @Test
    public void processMessageWithNoNumberDimension() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("greaterThanDimensionFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        NumberComparingFilter myFilter = (NumberComparingFilter) myFunc;

        Map<String, Object> message = new HashMap<>();

        message.put("ValueA", true);
        message.put("ValueB", null);
        assertFalse(myFilter.process("key1", message));

        message.put("ValueA", null);
        message.put("ValueB", "NaN");
        assertFalse(myFilter.process("key1", message));

        message.put("ValueA", "NaN");
        message.put("ValueB", false);
        assertFalse(myFilter.process("key1", message));
    }

    @Test
    public void processMessageWithEqualityInAllFilters() {
        Map<String, Object> message = new HashMap<>();
        message.put("ValueA", 2.5);
        message.put("ValueB", 2.5);

        Map<String, Function> functions = streamBuilder.getFunctions("stream1");

        // LOWER THAN DIMENSION FILTER
        Function myFunc = functions.get("lowerThanDimensionFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        NumberComparingFilter myFilter = (NumberComparingFilter) myFunc;

        assertFalse(myFilter.process("key1", message));

        // GREATER THAN DIMENSION FILTER
        myFunc = functions.get("greaterThanDimensionFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        myFilter = (NumberComparingFilter) myFunc;

        assertFalse(myFilter.process("key1", message));

        message.clear();
        message.put("ValueToCompare", 2.5);

        // LOWER THAN NUMBER FILTER
        myFunc = functions.get("lowerThanNumberFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        myFilter = (NumberComparingFilter) myFunc;

        assertFalse(myFilter.process("key1", message));

        // GREATER THAN NUMBER FILTER
        myFunc = functions.get("greaterThanNumberFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        myFilter = (NumberComparingFilter) myFunc;

        assertFalse(myFilter.process("key1", message));
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("greaterThanNumberFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        NumberComparingFilter myFilter = (NumberComparingFilter) myFunc;

        assertFalse(myFilter.process("key1", null));
    }

    @Test
    public void processNullKeyAndMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("greaterThanNumberFilter");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        NumberComparingFilter myFilter = (NumberComparingFilter) myFunc;

        assertFalse(myFilter.process(null, null));
    }

    @Test(expected = FunctionException.class)
    public void malformedNumberComparingFilter() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        StreamBuilder streamBuilder = new StreamBuilder(config, null);

        File file = new File(classLoader.getResource("malformed-number-comparing-filter.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }


    @AfterClass
    public static void stop() {
        streamBuilder.close();
    }

}

