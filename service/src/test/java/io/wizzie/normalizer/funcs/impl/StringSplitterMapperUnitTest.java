package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class StringSplitterMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "myApp");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("string-splitter-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringSplitterFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringSplitterMapper);
        StringSplitterMapper myStringReplaceMapper = (StringSplitterMapper) myFunc;

        assertEquals("DIM-H", myStringReplaceMapper.dimension);
        assertEquals(">", myStringReplaceMapper.delimiter);
        assertEquals(Arrays.asList("country", "province", "city"), myStringReplaceMapper.fieldNames);
    }

    @Test
    public void processSimpleMessage(){
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringSplitterFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringSplitterMapper);
        StringSplitterMapper myStringReplaceMapper = (StringSplitterMapper) myFunc;

        Map<String, Object> msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 94);
        msg.put("DIM-C", Arrays.asList("X", "Y", "Z"));
        msg.put("DIM-H", "myCountry > myProvince > myCity");

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("country", "myCountry");
        expectedMsg.put("province", "myProvince");
        expectedMsg.put("city", "myCity");

        assertEquals(new KeyValue<>("KEY", expectedMsg), myStringReplaceMapper.process("KEY", msg));
    }

    @Test
    public void processNullKey(){
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringSplitterFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringSplitterMapper);
        StringSplitterMapper myStringReplaceMapper = (StringSplitterMapper) myFunc;

        Map<String, Object> msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 94);
        msg.put("DIM-C", Arrays.asList("X", "Y", "Z"));
        msg.put("DIM-H", "myCountry > myProvince > myCity");

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("country", "myCountry");
        expectedMsg.put("province", "myProvince");
        expectedMsg.put("city", "myCity");

        assertEquals(new KeyValue<>(null, expectedMsg), myStringReplaceMapper.process(null, msg));
    }

    @Test
    public void processMoreSplitThanFields(){
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringSplitterFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringSplitterMapper);
        StringSplitterMapper myStringReplaceMapper = (StringSplitterMapper) myFunc;

        Map<String, Object> msg = new HashMap<>();
        msg.put("DIM-A", "VALUE-A");
        msg.put("DIM-B", 94);
        msg.put("DIM-C", Arrays.asList("X", "Y", "Z"));
        msg.put("DIM-H", "myCountry > myProvince > myCity > MoreSplit");

        Map<String, Object> expectedMsg = new HashMap<>();
        expectedMsg.putAll(msg);
        expectedMsg.put("country", "myCountry");
        expectedMsg.put("province", "myProvince");
        expectedMsg.put("city", "myCity > MoreSplit");

        assertEquals(new KeyValue<>(null, expectedMsg), myStringReplaceMapper.process(null, msg));
    }



    @Test
    public void processNullValueMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringSplitterFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringSplitterMapper);
        StringSplitterMapper myStringReplaceMapper = (StringSplitterMapper) myFunc;

        assertEquals(new KeyValue<>("KEY", null), myStringReplaceMapper.process("KEY", null));
    }

    @Test
    public void processNullKeyAndValueMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("myStream");
        Function myFunc = functions.get("myStringSplitterFunction");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof StringSplitterMapper);
        StringSplitterMapper myStringReplaceMapper = (StringSplitterMapper) myFunc;

        assertEquals(new KeyValue<>(null, null), myStringReplaceMapper.process(null, null));
    }

}
