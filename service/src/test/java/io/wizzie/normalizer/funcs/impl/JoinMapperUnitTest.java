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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class JoinMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("join-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myJoinMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        JoinMapper myJoinFunc = (JoinMapper) myFunc;

        assertEquals("-", myJoinFunc.delimiter);

        List<Map<String, Object>> values = new ArrayList<>();

        Map<String ,Object> value = new HashMap<>();
        value.put("fromDimension", "A");
        value.put("orDefault", "1");
        value.put("delete", false);

        values.add(value);

        value = new HashMap<>();
        value.put("fromDimension", "B");
        value.put("orDefault", "2");
        value.put("delete", true);

        values.add(value);

        value = new HashMap<>();
        value.put("fromDimension", "C");
        value.put("orDefault", "3");
        value.put("delete", true);

        values.add(value);

        value = new HashMap<>();
        value.put("fromDimension", "D");
        value.put("orDefault", "4");

        values.add(value);

        assertEquals(values, myJoinFunc.dimensionsToJoin);

        assertEquals("myDimension", myJoinFunc.newDimension);
    }


    @Test
    public void processNullMessage() {

        String KEY = "key";

        Function myFunc = streamBuilder.getFunctions("stream1").get("myJoinMapper");;

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        JoinMapper myJoinFunc = (JoinMapper) myFunc;

        KeyValue<String, Map<String, Object>> message = myJoinFunc.process(KEY, null);

        assertEquals(new KeyValue<>(KEY, new HashMap<>()), message);
    }

    @Test
    public void processNullKey() {

        Function myFunc = streamBuilder.getFunctions("stream1").get("myJoinMapper");;

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        JoinMapper myJoinFunc = (JoinMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "1");
        message.put("B", "2");
        message.put("C", "3");
        message.put("D", "4");

        KeyValue<String, Map<String, Object>> receivedMessage = myJoinFunc.process(null, message);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("A", "1");
        expectedMessage.put("D", "4");
        expectedMessage.put("myDimension", "1-2-3-4");

        assertEquals(new KeyValue<>(null, expectedMessage), receivedMessage);
    }

    @Test
    public void processNullKeyAndMessage() {

        Function myFunc = streamBuilder.getFunctions("stream1").get("myJoinMapper");;

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        JoinMapper myJoinFunc = (JoinMapper) myFunc;

        KeyValue<String, Map<String, Object>> message = myJoinFunc.process(null, null);

        assertEquals(new KeyValue<>(null, new HashMap<>()), message);
    }

    @Test
    public void processMessageWithValues() {

        String KEY = "key";

        Function myFunc = streamBuilder.getFunctions("stream1").get("myJoinMapper");;

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        JoinMapper myJoinFunc = (JoinMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "1");
        message.put("B", "2");
        message.put("C", "3");
        message.put("D", "4");

        KeyValue<String, Map<String, Object>> receivedMessage = myJoinFunc.process(KEY, message);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("A", "1");
        expectedMessage.put("D", "4");
        expectedMessage.put("myDimension", "1-2-3-4");

        assertEquals(new KeyValue<>(KEY, expectedMessage), receivedMessage);
    }

    @Test
    public void processMessageWithSomeValues() {

        String KEY = "key";

        Function myFunc = streamBuilder.getFunctions("stream1").get("myJoinMapper");;

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        JoinMapper myJoinFunc = (JoinMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("A", "1");
        message.put("D", "4");

        KeyValue<String, Map<String, Object>> receivedMessage = myJoinFunc.process(KEY, message);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("A", "1");
        expectedMessage.put("D", "4");
        expectedMessage.put("myDimension", "1-2-3-4");

        assertEquals(new KeyValue<>(KEY, expectedMessage), receivedMessage);
    }

    @AfterClass
    public static void stop(){
        streamBuilder.close();
    }
}
