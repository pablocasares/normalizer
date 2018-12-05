

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

public class FieldMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("field-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFieldMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        FieldMapper myFieldMapper = (FieldMapper) myFunc;

        assertNotNull(myFieldMapper.dimensionsToAdd);

        List<Map<String, Object>> dimensions = new LinkedList<>();

        Map<String, Object> value = new HashMap<>();
        value.put("dimension", "dimension1");
        value.put("value", "defaultValue1");
        value.put("overwrite", false);

        dimensions.add(value);

        value = new HashMap<>();
        value.put("dimension", "dimension2");
        value.put("value", "defaultValue2");
        value.put("overwrite", true);

        dimensions.add(value);

        value = new HashMap<>();
        value.put("dimension", "dimension3");
        value.put("value", "defaultValue3");

        dimensions.add(value);


        assertEquals(dimensions, myFieldMapper.dimensionsToAdd);
    }

    @Test
    public void processSimpleMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFieldFunc = functions.get("myFieldMapper");

        assertNotNull(myFieldFunc);
        assertTrue(myFieldFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myFieldFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 123456789L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "defaultValue2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFieldFunc = functions.get("myFieldMapper");

        assertNotNull(myFieldFunc);
        assertTrue(myFieldFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myFieldFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 123456789L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "defaultValue2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process(null, message1);

        assertEquals(new KeyValue<>(null, expectedMessage1), result1);

    }

    @Test
    public void processNullKeyAndMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFieldFunc = functions.get("myFieldMapper");

        assertNotNull(myFieldFunc);
        assertTrue(myFieldFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myFieldFunc;

        Map<String, Object> message1 = null;

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process(null, message1);

        assertEquals(new KeyValue<>(null, null), result1);

    }


    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFieldFunc = functions.get("myFieldMapper");

        assertNotNull(myFieldFunc);
        assertTrue(myFieldFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myFieldFunc;

        Map<String, Object> message1 = null;


        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process("KEY", message1);

        Map<String, Object> expectedMessage1 = null;

        assertEquals(new KeyValue<>("KEY", null), result1);

    }

    @Test
    public void processNestedMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFieldFunc = functions.get("myFieldMapper");

        assertNotNull(myFieldFunc);
        assertTrue(myFieldFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myFieldFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("dimension1", "{\"key1\":{\"key11\":\"value1\"}}");
        message1.put("dimension2", "{\"key2\":{\"key22\":\"value2\"}}");
        message1.put("dimension3", "value3");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 123456789L);
        expectedMessage1.put("dimension1", "{\"key1\":{\"key11\":\"value1\"}}");
        expectedMessage1.put("dimension2", "defaultValue2");
        expectedMessage1.put("dimension3", "value3");

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);

    }

    @Test
    public void processNullDimension() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFieldFunc = functions.get("myFieldMapper");

        assertNotNull(myFieldFunc);
        assertTrue(myFieldFunc instanceof MapperFunction);
        MapperFunction myReplaceMapper = (MapperFunction) myFieldFunc;

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("dimension1", "value1");
        message1.put("dimension2", "value2");


        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 123456789L);
        expectedMessage1.put("dimension1", "value1");
        expectedMessage1.put("dimension2", "defaultValue2");
        expectedMessage1.put("dimension3", "defaultValue3");

        KeyValue<String, Map<String, Object>> result1 = myReplaceMapper.process("KEY", message1);

        assertEquals(new KeyValue<>("KEY", expectedMessage1), result1);
    }

    @AfterClass
    public static void stop() {
        streamBuilder.close();
    }

}


