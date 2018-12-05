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

public class ClassificationMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("classification-mapper.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);

        streamBuilder.builder(model);
        Map<String, Function> functionsMap = streamBuilder.getFunctions("myStream");
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");

        Function myFuncKey = functions.get("myClassificationMapper");

        assertNotNull(myFuncKey);
        assertTrue(myFuncKey instanceof MapperFunction);
        ClassificationMapper myClassificationMapper = (ClassificationMapper) myFuncKey;

        assertNotNull(myClassificationMapper.dimension);
        assertNotNull(myClassificationMapper.targetDimension);
        assertNotNull(myClassificationMapper.classification);
        assertNotNull(myClassificationMapper.intervals);
        assertNotNull(myClassificationMapper.unknownValue);
    }

    @Test
    public void processAllIntervalsMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");

        Function myFuncKey = functions.get("myClassificationMapper");

        assertNotNull(myFuncKey);
        assertTrue(myFuncKey instanceof MapperFunction);
        ClassificationMapper myClassificationMapper = (ClassificationMapper) myFuncKey;

        Map<String, Object> msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 71);
        msg.put("DIM-C", -62);

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("NEW-DIM", "good");

        assertEquals(new KeyValue<>("KEY", expectedMsg), myClassificationMapper.process("KEY", msg));

        msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 71);
        msg.put("DIM-C", 0);

        expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("NEW-DIM", "unknown");

        assertEquals(new KeyValue<>("KEY", expectedMsg), myClassificationMapper.process("KEY", msg));

        msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 71);
        msg.put("DIM-C", -30);

        expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("NEW-DIM", "excellent");

        assertEquals(new KeyValue<>("KEY", expectedMsg), myClassificationMapper.process("KEY", msg));

        msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 71);
        msg.put("DIM-C", -100);

        expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("NEW-DIM", "bad");

        assertEquals(new KeyValue<>("KEY", expectedMsg), myClassificationMapper.process("KEY", msg));

        msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 71);
        msg.put("DIM-C", -83);

        expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("NEW-DIM", "low");

        assertEquals(new KeyValue<>("KEY", expectedMsg), myClassificationMapper.process("KEY", msg));

        msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 71);
        msg.put("DIM-C", -75);

        expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("NEW-DIM", "medium");

        assertEquals(new KeyValue<>("KEY", expectedMsg), myClassificationMapper.process("KEY", msg));
    }

    @Test
    public void processNullValue() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");

        Function myFuncKey = functions.get("myClassificationMapper");

        assertNotNull(myFuncKey);
        assertTrue(myFuncKey instanceof MapperFunction);
        ClassificationMapper myClassificationMapper = (ClassificationMapper) myFuncKey;

        assertEquals(new KeyValue<>("KEY", null), myClassificationMapper.process("KEY", null));
    }

    @Test
    public void processNullValueToClassify() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");

        Function myFuncKey = functions.get("myClassificationMapper");

        assertNotNull(myFuncKey);
        assertTrue(myFuncKey instanceof MapperFunction);
        ClassificationMapper myClassificationMapper = (ClassificationMapper) myFuncKey;

        Map<String, Object> msg = new HashMap<>();
        msg.put("DIM-C", null);

        Map<String, Object> msgExpected= new HashMap<>();
        msgExpected.put("DIM-C", null);
        msgExpected.put("NEW-DIM", "unknown");

        assertEquals(msgExpected, myClassificationMapper.process("KEY", msg).value);
    }

    @Test
    public void processNullKeyAndValue() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");

        Function myFuncKey = functions.get("myClassificationMapper");

        assertNotNull(myFuncKey);
        assertTrue(myFuncKey instanceof MapperFunction);
        ClassificationMapper myClassificationMapper = (ClassificationMapper) myFuncKey;

        assertEquals(new KeyValue<>(null, null), myClassificationMapper.process(null, null));
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");

        Function myFuncKey = functions.get("myClassificationMapper");

        assertNotNull(myFuncKey);
        assertTrue(myFuncKey instanceof MapperFunction);
        ClassificationMapper myClassificationMapper = (ClassificationMapper) myFuncKey;

        Map<String, Object> msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 71);
        msg.put("DIM-C", -62);

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("NEW-DIM", "good");

        assertEquals(new KeyValue<>(null, expectedMsg), myClassificationMapper.process(null, msg));
    }

    @AfterClass
    public static void stop(){
        streamBuilder.close();
    }

}
