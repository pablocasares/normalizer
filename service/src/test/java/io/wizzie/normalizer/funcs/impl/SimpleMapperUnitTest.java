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
import java.util.Map;

import static org.junit.Assert.*;

public class SimpleMapperUnitTest {
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);
    private static StreamBuilder streamBuilder2 = new StreamBuilder(config, null);
    private static StreamBuilder streamBuilder3 = new StreamBuilder(config, null);
    private static StreamBuilder streamBuilder4 = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("simple-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);

        File file2 = new File(classLoader.getResource("simple-mapper-delete.json").getFile());
        PlanModel model2 = objectMapper.readValue(file2, PlanModel.class);
        streamBuilder2.builder(model2);

        File file3 = new File(classLoader.getResource("simple-mapper-delete-with-empty.json").getFile());
        PlanModel model3 = objectMapper.readValue(file3, PlanModel.class);
        streamBuilder3.builder(model3);

        File file4 = new File(classLoader.getResource("simple-mapper-delete-two-fields.json").getFile());
        PlanModel model4 = objectMapper.readValue(file4, PlanModel.class);
        streamBuilder4.builder(model4);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        assertEquals(Arrays.asList("A", "B", "C"), myMapper.mappers.get(0).getDimPath());
        assertEquals("X", myMapper.mappers.get(0).getAs());

        assertEquals(Arrays.asList("Y", "W", "Z"), myMapper.mappers.get(1).getDimPath());
        assertEquals("Q", myMapper.mappers.get(1).getAs());

        assertEquals(Arrays.asList("Y", "W", "P"), myMapper.mappers.get(2).getDimPath());
        assertEquals("P", myMapper.mappers.get(2).getAs());

        assertEquals(Arrays.asList("timestamp"), myMapper.mappers.get(3).getDimPath());
        assertEquals("timestamp", myMapper.mappers.get(3).getAs());
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);

        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        b.put("C", "TEST-C");
        a.put("B", b);
        message.put("A", a);


        Map<String, Object> y = new HashMap<>();
        Map<String, Object> w = new HashMap<>();
        w.put("Z", "TEST-Z");
        w.put("P", "TEST-P");
        y.put("W", w);
        message.put("Y", y);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("TEST-C", value.get("X"));
        assertEquals("TEST-Z", value.get("Q"));
        assertEquals("TEST-P", value.get("P"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processMessageWithoutDimension() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);

        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("B", b);
        message.put("A", a);


        Map<String, Object> y = new HashMap<>();
        Map<String, Object> w = new HashMap<>();
        w.put("Z", "TEST-Z");
        w.put("P", "TEST-P");
        y.put("W", w);
        message.put("Y", y);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertFalse(value.containsKey("X"));
        assertEquals("TEST-Z", value.get("Q"));
        assertEquals("TEST-P", value.get("P"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processSimpleMessageShouldWork() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);

        message.put("A", "shouldWork");


        Map<String, Object> y = new HashMap<>();
        Map<String, Object> w = new HashMap<>();
        w.put("Z", "TEST-Z");
        w.put("P", "TEST-P");
        y.put("W", w);
        message.put("Y", y);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("TEST-Z", value.get("Q"));
        assertEquals("TEST-P", value.get("P"));
        assertEquals(123456789, value.get("timestamp"));
        assertEquals(3, value.keySet().size());
    }

    @Test
    public void processSimpleMessageDeleteMode() {
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);

        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        b.put("C", "TEST-C");
        a.put("B", b);
        message.put("A", a);


        Map<String, Object> y = new HashMap<>();
        Map<String, Object> w = new HashMap<>();
        w.put("Z", "TEST-Z");
        w.put("P", "TEST-P");
        y.put("W", w);
        message.put("Y", y);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);
        Map<String, Object> value = mapMessage.value;

        Map<String, Object> messageExpected = new HashMap<>();
        Map<String, Object> yExpected = new HashMap<>();
        Map<String, Object> wExpected = new HashMap<>();
        wExpected.put("P", "TEST-P");
        yExpected.put("W", wExpected);
        assertEquals(yExpected, value.get("Y"));
        assertEquals(123456789, value.get("timestamp"));
        assertEquals(2, value.keySet().size());
    }

    @Test
    public void processSimpleMessageDeleteModeTwoFields() {
        Map<String, Function> functions = streamBuilder4.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("value",2);
        message.put("other", "value");

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);
        Map<String, Object> value = mapMessage.value;

        Map<String, Object> messageExpected = new HashMap<>();
        messageExpected.put("other", "value");

        assertEquals(messageExpected, value);
        assertEquals(1, value.keySet().size());
    }


    @Test
    public void processSimpleMessageDeleteModeShouldWork() {
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);


        message.put("A", "shouldWork");


        Map<String, Object> y = new HashMap<>();
        Map<String, Object> w = new HashMap<>();
        w.put("Z", "TEST-Z");
        w.put("P", "TEST-P");
        y.put("W", w);
        message.put("Y", y);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);
        Map<String, Object> value = mapMessage.value;

        Map<String, Object> messageExpected = new HashMap<>();
        Map<String, Object> yExpected = new HashMap<>();
        Map<String, Object> wExpected = new HashMap<>();
        wExpected.put("P", "TEST-P");
        yExpected.put("W", wExpected);
        assertEquals(yExpected, value.get("Y"));
        assertEquals(123456789, value.get("timestamp"));
        assertEquals("shouldWork", value.get("A"));
        assertEquals(3, value.keySet().size());
    }

    @Test
    public void processSimpleMessageDeleteWithoutEmptyDeleteMode() {
        Map<String, Function> functions = streamBuilder3.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);

        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        b.put("C", "TEST-C");
        a.put("B", b);
        message.put("A", a);


        Map<String, Object> y = new HashMap<>();
        Map<String, Object> w = new HashMap<>();
        w.put("Z", "TEST-Z");
        w.put("P", "TEST-P");
        y.put("W", w);
        message.put("Y", y);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);
        Map<String, Object> value = mapMessage.value;

        Map<String, Object> yExpected = new HashMap<>();
        Map<String, Object> wExpected = new HashMap<>();
        wExpected.put("P", "TEST-P");
        yExpected.put("W", wExpected);
        assertEquals(yExpected, value.get("Y"));

        Map<String, Object> bExpected = new HashMap<>();
        bExpected.put("B", new HashMap<>());
        assertEquals(bExpected, value.get("A"));
        assertEquals(123456789, value.get("timestamp"));
        assertEquals(3, value.keySet().size());
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);

        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        b.put("C", "TEST-C");
        a.put("B", b);
        message.put("A", a);

        Map<String, Object> y = new HashMap<>();
        Map<String, Object> w = new HashMap<>();
        w.put("Z", "TEST-Z");
        w.put("P", "TEST-P");
        y.put("W", w);
        message.put("Y", y);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process(null, message);
        assertNull(mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("TEST-C", value.get("X"));
        assertEquals("TEST-Z", value.get("Q"));
        assertEquals("TEST-P", value.get("P"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processNullKeyAndNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process(null, null);
        assertNull(mapMessage.key);
        assertNull(mapMessage.value);
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof SimpleMapper);
        SimpleMapper myMapper = (SimpleMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("KEY", null);
        assertEquals("KEY", mapMessage.key);
        assertNull(mapMessage.value);
    }

    @Test
    public void toStringTest() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertEquals("[ {dimPath: [A, B, C], as: X}  {dimPath: [Y, W, Z], as: Q} " +
                " {dimPath: [Y, W, P], as: P}  {dimPath: [timestamp], as: timestamp} ]", myFunc.toString());
    }

    @AfterClass
    public static void stopTest() {
        streamBuilder.close();
    }
}
