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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RegexMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("regex-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myRegexMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        RegexMapper myRegexMapper = (RegexMapper) myFunc;

        assertEquals(Arrays.asList("dim1", "dim2", "dim3"), myRegexMapper.generateDimensions);
        assertEquals("test test (?<dim1>.*) test (?<dim2>.*) test (?<dim3>.*) test", myRegexMapper.pattern.pattern());
        assertEquals("message", myRegexMapper.parseDimension);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myRegexMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        RegexMapper myRegexMapper = (RegexMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("message", "test test hello1 test hello2 test hello3 test");

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("timestamp", 123456789L);
        expectedMessage.put("message", "test test hello1 test hello2 test hello3 test");
        expectedMessage.put("dim1", "hello1");
        expectedMessage.put("dim2", "hello2");
        expectedMessage.put("dim3", "hello3");

        assertEquals(new KeyValue<>("AAA", expectedMessage), myRegexMapper.process("AAA", message));
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myRegexMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        RegexMapper myRegexMapper = (RegexMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("message", "test test hello1 test hello2 test hello3 test");

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("timestamp", 123456789L);
        expectedMessage.put("message", "test test hello1 test hello2 test hello3 test");
        expectedMessage.put("dim1", "hello1");
        expectedMessage.put("dim2", "hello2");
        expectedMessage.put("dim3", "hello3");

        assertEquals(new KeyValue<>(null, expectedMessage), myRegexMapper.process(null, message));
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myRegexMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        RegexMapper myRegexMapper = (RegexMapper) myFunc;

        assertEquals(new KeyValue<>("AAA", null), myRegexMapper.process("AAA", null));
    }

    @Test
    public void processNullKeyAndMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myRegexMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        RegexMapper myRegexMapper = (RegexMapper) myFunc;

        Map<String, Object> message1 = null;

        Map<String, Object> expectedMessage1 = null;

        KeyValue<String, Map<String, Object>> result1 = myRegexMapper.process(null, message1);

        assertEquals(new KeyValue<>(null, expectedMessage1), result1);

    }

    @Test
    public void processNullParserDimension() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myRegexMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        RegexMapper myRegexMapper = (RegexMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("A", "VALUE-A");

        KeyValue<String, Map<String, Object>> result = myRegexMapper.process("KEY", message);

        assertEquals(new KeyValue<>("KEY", message), result);
    }

    @AfterClass
    public static void stop(){
        streamBuilder.close();
    }

}
