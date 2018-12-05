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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class MapFlattenMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("map-flatten-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMapFlattenMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MapFlattenMapper myMapFlattenFunction = (MapFlattenMapper) myFunc;

        assertEquals("data", myMapFlattenFunction.flatDimension);
        assertEquals("key_dim", myMapFlattenFunction.keyDimension);
        assertEquals("array_data", myMapFlattenFunction.outputDimension);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMapFlattenMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MapFlattenMapper myMapFlattenFunction = (MapFlattenMapper) myFunc;

        Map<String, Object> msg = new HashMap<>();
        msg.put("timestamp", 1234567890);
        msg.put("mac", "Ab1231cEf");

        Map<String, Object> data = new HashMap<>();

        Map<String, Object> msg1 = new HashMap<>();
        msg1.put("dim", "AA");
        msg1.put("dim1", "BB");

        Map<String, Object> msg2 = new HashMap<>();
        msg2.put("dim", "ZZ");
        msg2.put("dim1", "YY");

        data.put("msg1", msg1);
        data.put("msg2", msg2);

        msg.put("data", data);

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.put("timestamp", 1234567890);
        expectedMsg.put("mac", "Ab1231cEf");

        Map<String, Object> expectedmsg1 = new HashMap<>();
        expectedmsg1.put("key_dim", "msg1");
        expectedmsg1.put("dim", "AA");
        expectedmsg1.put("dim1", "BB");

        Map<String, Object> expectedmsg2 = new HashMap<>();
        expectedmsg2.put("key_dim", "msg2");
        expectedmsg2.put("dim", "ZZ");
        expectedmsg2.put("dim1", "YY");

        List<Map<String, Object>> arrayList = new ArrayList<>();

        arrayList.add(expectedmsg2);
        arrayList.add(expectedmsg1);

        expectedMsg.put("array_data", arrayList);

        assertEquals(new KeyValue<>("KEY-A", expectedMsg), myMapFlattenFunction.process("KEY-A", msg));
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMapFlattenMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MapFlattenMapper myMapFlattenFunction = (MapFlattenMapper) myFunc;

        Map<String, Object> msg = new HashMap<>();
        msg.put("timestamp", 1234567890);
        msg.put("mac", "Ab1231cEf");

        Map<String, Object> data = new HashMap<>();

        Map<String, Object> msg1 = new HashMap<>();
        msg1.put("dim", "AA");
        msg1.put("dim1", "BB");

        Map<String, Object> msg2 = new HashMap<>();
        msg2.put("dim", "ZZ");
        msg2.put("dim1", "YY");

        data.put("msg1", msg1);
        data.put("msg2", msg2);

        msg.put("data", data);

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.put("timestamp", 1234567890);
        expectedMsg.put("mac", "Ab1231cEf");

        Map<String, Object> expectedmsg1 = new HashMap<>();
        expectedmsg1.put("key_dim", "msg1");
        expectedmsg1.put("dim", "AA");
        expectedmsg1.put("dim1", "BB");

        Map<String, Object> expectedmsg2 = new HashMap<>();
        expectedmsg2.put("key_dim", "msg2");
        expectedmsg2.put("dim", "ZZ");
        expectedmsg2.put("dim1", "YY");

        List<Map<String, Object>> arrayList = new ArrayList<>();

        arrayList.add(expectedmsg2);
        arrayList.add(expectedmsg1);

        expectedMsg.put("array_data", arrayList);

        assertEquals(new KeyValue<>(null, expectedMsg), myMapFlattenFunction.process(null, msg));
    }

    @Test
    public void processNullMessages() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMapFlattenMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MapFlattenMapper myMapFlattenFunction = (MapFlattenMapper) myFunc;

        assertEquals(new KeyValue<>("key", null), myMapFlattenFunction.process("key", null));
    }

    @Test
    public void processNullKeysAndMessages() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMapFlattenMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MapFlattenMapper myMapFlattenFunction = (MapFlattenMapper) myFunc;

        assertEquals(new KeyValue<>(null, null), myMapFlattenFunction.process(null, null));
    }

    @Test
    public void processNullDimensionMessages() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myMapFlattenMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        MapFlattenMapper myMapFlattenFunction = (MapFlattenMapper) myFunc;

        Map<String, Object> msg = new HashMap<>();
        msg.put("timestamp", 1234567890);

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.put("timestamp", 1234567890);

        assertEquals(new KeyValue<>(null, expectedMsg), myMapFlattenFunction.process(null, msg));
    }

}
