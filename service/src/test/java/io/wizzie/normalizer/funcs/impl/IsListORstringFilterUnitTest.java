package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.FilterFunc;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class IsListORstringFilterUnitTest {
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("IsListORstringFilter.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function isListFunc = functions.get("isList");

        assertNotNull(isListFunc);
        assertTrue(isListFunc instanceof FilterFunc);
        IsListFilter isListFilter = (IsListFilter) isListFunc;
        assertEquals("list-dimension", isListFilter.dimension);

        Function isStringFunc = functions.get("isString");

        assertNotNull(isStringFunc);
        assertTrue(isStringFunc instanceof FilterFunc);
        IsStringFilter isStringFilter = (IsStringFilter) isStringFunc;
        assertEquals("string-dimension", isStringFilter.dimension);
    }

    @Test
    public void filterListMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("isList");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        IsListFilter isListFilter = (IsListFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put(isListFilter.dimension, Arrays.asList("a", "b", "c"));

        assertTrue(isListFilter.process("key1", message));

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put(isListFilter.dimension, "NOT-FILTER-VALUE");

        assertFalse(isListFilter.process("key1", message1));
    }

    @Test
    public void filterStringMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("isString");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        IsStringFilter isStringFilter = (IsStringFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put(isStringFilter.dimension, Arrays.asList("a", "b", "c"));

        assertFalse(isStringFilter.process("key1", message));

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put(isStringFilter.dimension, "NOT-FILTER-VALUE");

        assertTrue(isStringFilter.process("key1", message1));
    }

    @Test
    public void processStringNullDimension() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("isString");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        IsStringFilter myFilter = (IsStringFilter) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);

        assertFalse(myFilter.process("key1", message));

        myFunc = functions.get("isList");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        IsListFilter isListFilter = (IsListFilter) myFunc;

        assertFalse(isListFilter.process("key1", message));

    }

    @Test
    public void processListNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("isList");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        IsListFilter isListFilter = (IsListFilter) myFunc;

        Map<String, Object> message = null;

        assertFalse(isListFilter.process("key1", message));

        myFunc = functions.get("isString");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FilterFunc);
        IsStringFilter isStringFilter = (IsStringFilter) myFunc;

        assertFalse(isStringFilter.process("key1", message));
    }

    @AfterClass
    public static void stop(){
        streamBuilder.close();
    }
}
