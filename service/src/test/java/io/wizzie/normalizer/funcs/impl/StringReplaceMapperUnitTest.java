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
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class StringReplaceMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("string-replace-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringReplacementFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringReplaceMapper);
        StringReplaceMapper myStringReplaceMapper = (StringReplaceMapper) myFunc;

        assertEquals("DIM-C", myStringReplaceMapper.dimension);
        assertEquals("-", myStringReplaceMapper.targetString);
        assertEquals(".", myStringReplaceMapper.replacementString);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringReplacementFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringReplaceMapper);
        StringReplaceMapper myStringReplaceMapper = (StringReplaceMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("DIM-A", "VALUE-A");
        message.put("DIM-B", 77);
        message.put("DIM-C", "192-168-1-177");

        KeyValue<String, Map<String, Object>> mapMessage = myStringReplaceMapper.process("key1", message);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("timestamp", 123456789);
        expectedMessage.put("DIM-A", "VALUE-A");
        expectedMessage.put("DIM-B", 77);
        expectedMessage.put("DIM-C", "192.168.1.177");

        assertEquals(new KeyValue<>("key1", expectedMessage), mapMessage);
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringReplacementFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringReplaceMapper);
        StringReplaceMapper myStringReplaceMapper = (StringReplaceMapper) myFunc;


        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("DIM-A", "VALUE-A");
        message.put("DIM-B", 77);
        message.put("DIM-C", "192-168-1-177");

        KeyValue<String, Map<String, Object>> mapMessage = myStringReplaceMapper.process(null, message);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("timestamp", 123456789);
        expectedMessage.put("DIM-A", "VALUE-A");
        expectedMessage.put("DIM-B", 77);
        expectedMessage.put("DIM-C", "192.168.1.177");

        assertNull(mapMessage.key);
        assertEquals(new KeyValue<>(null, expectedMessage), mapMessage);
    }

    @Test
    public void processNullKeyAndNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringReplacementFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringReplaceMapper);
        StringReplaceMapper myStringReplaceMapper = (StringReplaceMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myStringReplaceMapper.process(null, null);

        assertNull(mapMessage.key);
        assertNull(mapMessage.value);
        assertEquals(new KeyValue<>(null, null), mapMessage);
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringReplacementFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringReplaceMapper);
        StringReplaceMapper myStringReplaceMapper = (StringReplaceMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myStringReplaceMapper.process("key", null);

        assertEquals("key", mapMessage.key);
        assertNull(mapMessage.value);
        assertEquals(new KeyValue<>("key", null), mapMessage);
    }

    @AfterClass
    public static void stopTest(){
        streamBuilder.close();
    }

}
