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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ReplaceMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("replace-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myReplaceMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        ReplaceMapper myReplaceMapper = (ReplaceMapper) myFunc;

        List<Map<String, Object>> replacements = new LinkedList<>();
        Map<String, Map<Object, Object>> replacementsMap = new HashMap<>();

        Map<String, Object> replacement1 = new HashMap<>();
        replacement1.put("from", "ver");
        replacement1.put("to", "version");
        Map<String, Object> replacement2 = new HashMap<>();
        replacement2.put("from", "v");
        replacement2.put("to", "version");
        Map<String, Object> replacement3 = new HashMap<>();
        replacement3.put("from", "vrsn");
        replacement3.put("to", "version");
        Map<String, Object> replacement4 = new HashMap<>();
        replacement4.put("from", 9);
        replacement4.put("to", 10);
        Map<String, Object> replacement5 = new HashMap<>();
        replacement5.put("from", "nine");
        replacement5.put("to", 9);

        List<Map<String, Object>> replacementList  = new LinkedList<>();
        replacementList.add(replacement1);
        replacementList.add(replacement2);
        replacementList.add(replacement3);
        replacementList.add(replacement4);
        replacementList.add(replacement5);

        Map<String, Object> replace1 = new HashMap<>();
        replace1.put("dimension", "TYPE");
        replace1.put("replacements", replacementList);

        replacements.add(replace1);
        assertEquals(replacements, myReplaceMapper.replacements);

        Map<Object, Object> allReplacements = new HashMap<>();
        allReplacements.put("vrsn", "version");
        allReplacements.put("v", "version");
        allReplacements.put("ver", "version");
        allReplacements.put(9, 10);
        allReplacements.put("nine", 9);

        replacementsMap.put("TYPE", allReplacements);

        assertEquals(replacementsMap, myReplaceMapper.replacementsMap);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myReplaceFunc = functions.get("myReplaceMapper");

        assertNotNull(myReplaceFunc);
        assertTrue(myReplaceFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myReplaceFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("TYPE", "ver");
        message1.put("A", "VALUE-A");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 123456789L);
        expectedMessage1.put("TYPE", "version");
        expectedMessage1.put("A", "VALUE-A");

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("timestamp", 123456789L);
        message2.put("TYPE", "vrsn");
        message2.put("B", "VALUE-B");

        Map<String, Object> expectedMessage2 = new HashMap<>();
        expectedMessage2.put("timestamp", 123456789L);
        expectedMessage2.put("TYPE", "version");
        expectedMessage2.put("B", "VALUE-B");

        KeyValue<String, Map<String, Object>> result2 = myReplaceMapper.process("KEY", message2);

        assertEquals(new KeyValue<>("KEY", expectedMessage2), result2);

        Map<String, Object> message3 = new HashMap<>();
        message3.put("timestamp", 123456789L);
        message3.put("TYPE", 9);
        message3.put("B", "VALUE-B");

        Map<String, Object> expectedMessage3 = new HashMap<>();
        expectedMessage3.put("timestamp", 123456789L);
        expectedMessage3.put("TYPE", 10);
        expectedMessage3.put("B", "VALUE-B");

        KeyValue<String, Map<String, Object>> result3 = myReplaceMapper.process("KEY", message3);

        assertEquals(new KeyValue<>("KEY", expectedMessage3), result3);

        Map<String, Object> message4 = new HashMap<>();
        message4.put("timestamp", 123456789L);
        message4.put("TYPE", "nine");
        message4.put("B", "VALUE-B");

        Map<String, Object> expectedMessage4 = new HashMap<>();
        expectedMessage4.put("timestamp", 123456789L);
        expectedMessage4.put("TYPE", 9);
        expectedMessage4.put("B", "VALUE-B");

        KeyValue<String, Map<String, Object>> result4 = myReplaceMapper.process("KEY", message4);

        assertEquals(new KeyValue<>("KEY", expectedMessage4), result4);

        Map<String, Object> message5 = new HashMap<>();
        message5.put("timestamp", 123456789L);
        message5.put("TYPE", null);
        message5.put("B", "VALUE-B");

        Map<String, Object> expectedMessage5 = new HashMap<>();
        expectedMessage5.put("timestamp", 123456789L);
        expectedMessage5.put("TYPE", null);
        expectedMessage5.put("B", "VALUE-B");

        KeyValue<String, Map<String, Object>> result5 = myReplaceMapper.process("KEY", message5);

        assertEquals(new KeyValue<>("KEY", expectedMessage5), result5);



    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myReplaceFunc = functions.get("myReplaceMapper");

        assertNotNull(myReplaceFunc);
        assertTrue(myReplaceFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myReplaceFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("TYPE", "ver");
        message1.put("A", "VALUE-A");

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 123456789L);
        expectedMessage1.put("TYPE", "version");
        expectedMessage1.put("A", "VALUE-A");

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process(null, message1);

        assertEquals(new KeyValue<>(null, expectedMessage1), result1);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("timestamp", 123456789L);
        message2.put("TYPE", "vrsn");
        message2.put("B", "VALUE-B");

        Map<String, Object> expectedMessage2 = new HashMap<>();
        expectedMessage2.put("timestamp", 123456789L);
        expectedMessage2.put("TYPE", "version");
        expectedMessage2.put("B", "VALUE-B");

        KeyValue<String, Map<String, Object>> result2 = myReplaceMapper.process(null, message2);

        assertEquals(new KeyValue<>(null, expectedMessage2), result2);
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myReplaceFunc = functions.get("myReplaceMapper");

        assertNotNull(myReplaceFunc);
        assertTrue(myReplaceFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myReplaceFunc;

        Map<String, Object> message1 = null;

        Map<String, Object> expectedMessage1 = null;

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processNullKeyAndMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myReplaceFunc = functions.get("myReplaceMapper");

        assertNotNull(myReplaceFunc);
        assertTrue(myReplaceFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myReplaceFunc;

        Map<String, Object> message1 = null;

        Map<String, Object> expectedMessage1 = null;

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process(null, message1);

        assertEquals(new KeyValue<>(null, expectedMessage1), result1);

    }

    @Test
    public void processNullDimension() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myReplaceFunc = functions.get("myReplaceMapper");

        assertNotNull(myReplaceFunc);
        assertTrue(myReplaceFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myReplaceFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("TYPE", null);
        message.put("A", "VALUE-A");

        KeyValue<String, Map<String, Object>> result = myReplaceMapper.process("KEY", message);

        assertEquals(new KeyValue<>("KEY", message), result);
    }

    @Test
    public void processWithNulls() {

        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("replace-mapper-with-nulls.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = null;
        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            streamBuilder2.builder(model);
        } catch (PlanBuilderException e) {
            e.printStackTrace();
        }

        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myReplaceFunc = functions.get("myReplaceMapper");

        assertNotNull(myReplaceFunc);
        assertTrue(myReplaceFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myReplaceFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("TYPE", null);
        message.put("A", "VALUE-A");

        KeyValue<String, Map<String, Object>> result = myReplaceMapper.process("KEY", message);

        Map<String, Object> msgExpected = new HashMap<>();
        msgExpected.put("timestamp", 123456789L);
        msgExpected.put("TYPE", "version");
        msgExpected.put("A", "VALUE-A");

        assertEquals(msgExpected, result.value);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("timestamp", 123456789L);
        message2.put("TYPE", "v");
        message2.put("A", "VALUE-A");

        KeyValue<String, Map<String, Object>> result2 = myReplaceMapper.process("KEY", message2);

        Map<String, Object> msgExpected2 = new HashMap<>();
        msgExpected2.put("timestamp", 123456789L);
        msgExpected2.put("TYPE", null);
        msgExpected2.put("A", "VALUE-A");

        assertEquals(msgExpected2, result2.value);

        streamBuilder2.close();
    }

    @AfterClass
    public static void stop(){
        streamBuilder.close();
    }

}
