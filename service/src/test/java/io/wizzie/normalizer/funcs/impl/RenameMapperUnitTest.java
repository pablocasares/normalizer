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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RenameMapperUnitTest {
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("rename-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);

        assertTrue(myFunc instanceof RenameMapper);
        RenameMapper myMapper = (RenameMapper) myFunc;

        assertEquals(Collections.singletonList("C"), myMapper.mappers.get(0).getDimPath());
        assertEquals("X", myMapper.mappers.get(0).getAs());

        assertEquals(Collections.singletonList("Z"), myMapper.mappers.get(1).getDimPath());
        assertEquals("Q", myMapper.mappers.get(1).getAs());
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof RenameMapper);
        RenameMapper myMapper = (RenameMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("C", "TEST-C");
        message.put("Z", "lalala");
        message.put("B", "test");

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("TEST-C", value.get("X"));
        assertEquals("lalala", value.get("Q"));
        assertEquals("test", value.get("B"));
        assertEquals(123456789, value.get("timestamp"));
        assertNull(value.get("C"));
        assertNull(value.get("Z"));

    }

    @Test
    public void processSimpleMessageNoDelete() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream3");
        Function myFunc = functions.get("myMapper2");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof RenameMapper);
        RenameMapper myMapper = (RenameMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("C", "TEST-C");
        message.put("Z", "lalala");
        message.put("B", "test");

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("TEST-C", value.get("X"));
        assertEquals("lalala", value.get("Q"));
        assertEquals("test", value.get("B"));
        assertEquals(123456789, value.get("timestamp"));
        assertNotNull(value.get("C"));
        assertNotNull(value.get("Z"));

    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof RenameMapper);
        RenameMapper myMapper = (RenameMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("C", "TEST-C");
        message.put("Z", "lalala");
        message.put("B", "test");

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process(null, message);
        assertNull(mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals("TEST-C", value.get("X"));
        assertEquals("lalala", value.get("Q"));
        assertEquals("test", value.get("B"));
        assertEquals(123456789, value.get("timestamp"));
        assertNull(value.get("C"));
        assertNull(value.get("Z"));
    }

    @Test
    public void processNullKeyAndNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof RenameMapper);
        RenameMapper myMapper = (RenameMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process(null, null);
        assertNull(mapMessage.key);
        assertNull(mapMessage.value);
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof RenameMapper);
        RenameMapper myMapper = (RenameMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("KEY", null);
        assertEquals("KEY", mapMessage.key);
        assertNull(mapMessage.value);
    }

    @Test
    public void toStringTest() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertEquals("[ {dimPath: [C], as: X}  {dimPath: [Z], as: Q} ]", myFunc.toString());
    }

    @AfterClass
    public static void stopTest() {
        streamBuilder.close();
    }
}
