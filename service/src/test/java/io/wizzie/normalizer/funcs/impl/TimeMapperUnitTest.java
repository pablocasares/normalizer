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
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TimeMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-millis.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myTimeMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        TimeMapper myTimeMapper = (TimeMapper) myFunc;

        assertEquals("timestamp", myTimeMapper.dimensionToProcess);
        assertEquals("millis", myTimeMapper.toFormat);
        assertEquals("secs", myTimeMapper.fromFormat);
    }

    @Test
    public void processSecsToMillisReadedFromConfig() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890000L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890000L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process(null, message1);

        assertEquals(new KeyValue<>(null, expectedMessage1), result1);

    }


    @Test
    public void procesNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = null;

        Map<String, Object> expectedMessage1 = null;

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void procesNullKeyAndMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = null;

        Map<String, Object> expectedMessage1 = null;

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process(null, message1);

        assertEquals(new KeyValue<>(null, expectedMessage1), result1);

    }


    @Test
    public void processSecsToMillisNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-millis-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890000L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processNullSecsToMillisNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-millis-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processNullSecsToMillisReadedFromConfig() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);

        assertNotNull(result1.value.get("timestamp"));
    }

    @Test
    public void processSecsAsIntegerToMillisReadedFromConfig() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890000L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processSecsAsStringToMillisReadedFromConfig() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "1234567890");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890000L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processSecsToISOReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-ISO.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "2009-02-13T23:31:30.000Z");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processSecsToISONoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-ISO-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "2009-02-13T23:31:30.000Z");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processNullSecsToISONoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-ISO-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processNullSecsToISOReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-ISO.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertNotNull(result1.value.get("timestamp"));

    }

    @Test
    public void processSecsAsIntegerToISOReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-ISO.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "2009-02-13T23:31:30.000Z");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processSecsAsStringToISOReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-ISO.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "1234567890");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "2009-02-13T23:31:30.000Z");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);


    }


    @Test
    public void processMillisToSecsReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-secs.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890000L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processMillisToSecsNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-secs-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890000L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullMillisToSecsNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-secs-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullMillisToSecsReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-secs.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();

        assertNotNull(result1.value.get("timestamp"));
    }

    @Test
    public void processMillisAsStringToSecsReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-secs.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "1234567890000");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processMillisAsStringToSecsAsStringReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-secs-string.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "1234567890000");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "1234567890");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processMillisToISOReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-ISO.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890000L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "2009-02-13T23:31:30.000Z");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processMillisToISONoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-ISO-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890000L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "2009-02-13T23:31:30.000Z");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullMillisToISONoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-ISO-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullMillisToISOReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-ISO.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertNotNull(result1.value.get("timestamp"));
    }

    @Test
    public void processMillisAsStringToISOReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-ISO.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "1234567890000");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "2009-02-13T23:31:30.000Z");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processISOToSecsReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-secs.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "2009-02-13T23:31:30.000Z");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processISOToSecsNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-secs-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "2009-02-13T23:31:30.000Z");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullISOToSecsNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-secs-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullISOToSecsReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-secs.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();

        assertNotNull(result1.value.get("timestamp"));
    }


    @Test
    public void processISOToMillisReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-millis.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "2009-02-13T23:31:30.000Z");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890000L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processISOToMillisNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-millis-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "2009-02-13T23:31:30.000Z");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234567890000L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullISOToMillisNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-millis-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullISOToMillisReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-millis.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();

        assertNotNull(result1.value.get("timestamp"));
    }

    @Test
    public void processISOToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "2009-02-13T23:31:30.000Z");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090213");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processISOToPatternNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-pattern-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "2009-02-13T23:31:30.000Z");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090213");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullISOToPatternNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-pattern-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullISOToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-ISO-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertNotNull(result1.value.get("timestamp"));
    }

    @Test
    public void processMillisToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890000L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090213");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processMillisToPatternNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-pattern-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890000L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090213");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullMillisToPatternNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-pattern-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullMillisToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertNotNull(result1.value.get("timestamp"));
    }

    @Test
    public void processMillisAsStringToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-millis-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "1234567890000");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090213");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processSecsToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090213");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processSecsToPatternNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-pattern-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090213");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullSecsToPatternNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-pattern-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void procesNullSecsToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertNotNull(result1.value.get("timestamp"));
    }

    @Test
    public void processSecsAsIntegerToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 1234567890);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090213");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processSecsAsStringToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-secs-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "1234567890");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090213");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processPatternToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "2009-02-14");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090214");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processPatternToPatternNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-pattern-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "2009-02-14");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "20090214");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullPatternToPatternNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-pattern-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullPatternToPatternReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-pattern.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "2009-02-14");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertNotNull(result1.value.get("timestamp"));
    }

    @Test
    public void processPatternToMillisReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-millis.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "20090214");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234569600000L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processPatternToMillisNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-millis-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "20090214");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234569600000L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullPatternToMillisNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-millis-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullPatternToMillisReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-millis.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();

        assertNotNull(result1.value.get("timestamp"));
    }


    @Test
    public void processPatternToSecsReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-secs.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "20090214");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234569600L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processPatternToSecsNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-secs-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "20090214");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 1234569600L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullPatternToSecsNoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-secs-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processNullPatternToSecsReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-secs.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertNotNull(result1.value.get("timestamp"));
    }

    @Test
    public void processPatternToISOReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-ISO.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "20090214");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "2009-02-14T00:00:00.000Z");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @Test
    public void processPatternToISONoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-ISO-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", "20090214");
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", "2009-02-14T00:00:00.000Z");
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullPatternToISONoTimestampReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-ISO-no-timestamp.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "value2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }


    @Test
    public void processNullPatternToISOReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-pattern-to-ISO.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);
        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myTimeFunc = functions.get("myTimeMapper");

        assertNotNull(myTimeFunc);
        assertTrue(myTimeFunc instanceof MapperFunction);
        MapperFunction myTimeMapper = (MapperFunction) myTimeFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        KeyValue<String, Map<String, Object>> result1 = myTimeMapper.process("KEY", message1);
        streamBuilder2.close();
        assertNotNull(result1.value.get("timestamp"));
    }


    @Test
    public void processBadPropertiesReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-bad-properties.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        try {
            streamBuilder2.builder(model);
            fail();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void processNullPropertiesReadedFromConfig() throws IOException, PlanBuilderException {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("time-mapper-null-properties.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        try {
            streamBuilder2.builder(model);
            fail();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }


    @AfterClass
    public static void stop() {
        streamBuilder.close();
    }

}


