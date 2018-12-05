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
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class StartWithFilterUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("start-with-filter.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);

        streamBuilder.builder(model);
        Map<String, Function> functionsMap = streamBuilder.getFunctions("stream1");
    }


    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");

        Function myFuncKey = functions.get("myKeyStartWithFilter");

        assertNotNull(myFuncKey);
        assertTrue(myFuncKey instanceof FilterFunc);
        StartWithFilter myStartWithFilter = (StartWithFilter) myFuncKey;
        assertNotNull(myStartWithFilter.dimension);
        assertTrue(myStartWithFilter.isDimensionKey);
        assertEquals("FILTER", myStartWithFilter.startWithValue);

        Function myFuncValue = functions.get("myValueStartWithFilter");

        assertNotNull(myFuncValue);
        assertTrue(myFuncValue instanceof FilterFunc);
        StartWithFilter myFilterKey = (StartWithFilter) myFuncValue;
        assertNotNull(myFilterKey.dimension);
        assertEquals("TYPE", myFilterKey.dimension);
        assertFalse(myFilterKey.isDimensionKey);
        assertEquals("YES", myFilterKey.startWithValue);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");

        Function myKeyFunc = functions.get("myKeyStartWithFilter");

        assertNotNull(myKeyFunc);
        assertTrue(myKeyFunc instanceof FilterFunc);
        StartWithFilter myKeyStartWithFilter = (StartWithFilter) myKeyFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("TYPE", "YES-A");

        assertTrue(myKeyStartWithFilter.process("FILTER-KEY", message1));

        Map<String, Object> message2 = new HashMap<>();
        message2.put("timestamp", 123456789L);
        message2.put("TYPE", "NO-B");

        assertFalse(myKeyStartWithFilter.process("KEY", message1));

        Map<String, Object> message3 = new HashMap<>();
        message3.put("timestamp", 123456789L);
        message3.put("TYPE", "YES-C");

        Function myValueFunc = functions.get("myValueStartWithFilter");

        assertNotNull(myValueFunc);
        assertTrue(myValueFunc instanceof FilterFunc);
        StartWithFilter myValueStartWithFilter = (StartWithFilter) myValueFunc;

        assertTrue(myValueStartWithFilter.process("KEY", message1));
        assertFalse(myValueStartWithFilter.process("KEY", message2));
        assertTrue(myValueStartWithFilter.process("KEY", message3));
    }


    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");

        Function myKeyFunc = functions.get("myKeyStartWithFilter");

        assertNotNull(myKeyFunc);
        assertTrue(myKeyFunc instanceof FilterFunc);
        StartWithFilter myKeyStartWithFilter = (StartWithFilter) myKeyFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("TYPE", "YES-A");

        assertFalse(myKeyStartWithFilter.process(null, message1));

        Map<String, Object> message2 = new HashMap<>();
        message2.put("timestamp", 123456789L);
        message2.put("TYPE", "NO-B");

        assertFalse(myKeyStartWithFilter.process(null, message1));

        Map<String, Object> message3 = new HashMap<>();
        message3.put("timestamp", 123456789L);
        message3.put("TYPE", "YES-C");

        Function myValueFunc = functions.get("myValueStartWithFilter");

        assertNotNull(myValueFunc);
        assertTrue(myValueFunc instanceof FilterFunc);
        StartWithFilter myValueStartWithFilter = (StartWithFilter) myValueFunc;

        assertTrue(myValueStartWithFilter.process(null, message1));
        assertFalse(myValueStartWithFilter.process(null, message2));
        assertTrue(myValueStartWithFilter.process(null, message3));
    }


    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");

        Function myKeyFunc = functions.get("myKeyStartWithFilter");

        assertNotNull(myKeyFunc);
        assertTrue(myKeyFunc instanceof FilterFunc);
        StartWithFilter myKeyStartWithFilter = (StartWithFilter) myKeyFunc;

        Map<String, Object> message1 = null;

        assertFalse(myKeyStartWithFilter.process("FILTER-KEY", message1));

        Map<String, Object> message2 = null;
        assertFalse(myKeyStartWithFilter.process("KEY", message2));

        Map<String, Object> message3 = null;

        Function myValueFunc = functions.get("myValueStartWithFilter");

        assertNotNull(myValueFunc);
        assertTrue(myValueFunc instanceof FilterFunc);
        StartWithFilter myValueStartWithFilter = (StartWithFilter) myValueFunc;

        assertFalse(myValueStartWithFilter.process("KEY", message1));
        assertFalse(myValueStartWithFilter.process("KEY", message2));
        assertFalse(myValueStartWithFilter.process("KEY", message3));
    }

    @Test
    public void processNullKeyAndMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");

        Function myKeyFunc = functions.get("myKeyStartWithFilter");

        assertNotNull(myKeyFunc);
        assertTrue(myKeyFunc instanceof FilterFunc);
        StartWithFilter myKeyStartWithFilter = (StartWithFilter) myKeyFunc;

        Map<String, Object> message1 = null;

        assertFalse(myKeyStartWithFilter.process(null, message1));

        Function myValueFunc = functions.get("myValueStartWithFilter");

        assertNotNull(myValueFunc);
        assertTrue(myValueFunc instanceof FilterFunc);
        StartWithFilter myValueStartWithFilter = (StartWithFilter) myValueFunc;

        assertFalse(myValueStartWithFilter.process(null, message1));

    }

    @Test
    public void processNullDimension() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myKeyFunc = functions.get("myKeyStartWithFilter");

        assertNotNull(myKeyFunc);
        assertTrue(myKeyFunc instanceof FilterFunc);
        StartWithFilter myKeyStartWithFilter = (StartWithFilter) myKeyFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);

        assertFalse(myKeyStartWithFilter.process(null, message1));

        Function myValueFunc = functions.get("myValueStartWithFilter");

        assertNotNull(myValueFunc);
        assertTrue(myValueFunc instanceof FilterFunc);
        StartWithFilter myValueStartWithFilter = (StartWithFilter) myValueFunc;

        Map<String, Object> message2 = new HashMap<>();
        message2.put("timestamp", 123456789L);
        message2.put("TYPE", null);

        assertFalse(myValueStartWithFilter.process("KEY", message2));
    }

    @AfterClass
    public static void stop(){
        streamBuilder.close();
    }

}
