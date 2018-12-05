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
import java.util.*;

import static org.junit.Assert.*;

public class ArithmeticMapperUnitTest {
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("arithmetic-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;


        List<Map<String, Object>> equations = new LinkedList<>();

        Map <String,Object> equation1 = new HashMap<>();
        equation1.put("equation","field1+field2");
        equation1.put("as","sum");
        List<String> dimensions = new LinkedList<>();
        dimensions.add("field1");
        dimensions.add("field2");
        equation1.put("dimensions", dimensions);

        Map <String,Object> equation2 = new HashMap<>();
        equation2.put("equation","field1-field3");
        equation2.put("as","subtract");
        List<String> dimensions2 = new LinkedList<>();
        dimensions2.add("field1");
        dimensions2.add("field3");
        equation2.put("dimensions", dimensions2);

        Map <String,Object> equation3 = new HashMap<>();
        equation3.put("equation","field1*field2");
        equation3.put("as","multiply");
        List<String> dimensions3 = new LinkedList<>();
        dimensions3.add("field1");
        dimensions3.add("field2");
        equation3.put("dimensions", dimensions3);

        Map <String,Object> equation4 = new HashMap<>();
        equation4.put("equation","field1/field2");
        equation4.put("as","division");
        List<String> dimensions4 = new LinkedList<>();
        dimensions4.add("field1");
        dimensions4.add("field2");
        equation4.put("dimensions", dimensions4);

        Map <String,Object> equation5 = new HashMap<>();
        equation5.put("equation","sqrt(field1)");
        equation5.put("as","sqrt");
        List<String> dimensions5 = new LinkedList<>();
        dimensions5.add("field1");
        equation5.put("dimensions", dimensions5);


        equations.add(equation1);
        equations.add(equation2);
        equations.add(equation3);
        equations.add(equation4);
        equations.add(equation5);


        assertEquals(Arrays.asList(equations), Arrays.asList(myMapper.getEquations()));


        Map<String, Function> functions2 = streamBuilder.getFunctions("stream2");
        Function myFunc2 = functions2.get("myMapper2");

        assertNotNull(myFunc2);
        assertTrue(myFunc2 instanceof ArithmeticMapper);
        ArithmeticMapper myMapper2 = (ArithmeticMapper) myFunc2;

        List<Map<String, Object>> equations2 = new LinkedList<>();
        Map <String,Object> equation6 = new HashMap<>();
        equation6.put("equation","floor(field1*1000)/1000");
        equation6.put("as","truncate");
        List<String> dimensions6 = new LinkedList<>();
        dimensions6.add("field1");
        equation6.put("dimensions", dimensions6);
        equations2.add(equation6);
        assertEquals(Arrays.asList(equations2), Arrays.asList(myMapper2.getEquations()));


    }

    @Test
    public void processSumMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("field1", 7);
        message.put("field2", 0.5);
        message.put("field3", 1.5);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals(7.5, value.get("sum"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processStringFieldMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("field1", "7");
        message.put("field2", 0.5);
        message.put("field3", 1.5);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals(message, value);
    }

    @Test
    public void processSubtractMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("field1", 7);
        message.put("field2", 0.5);
        message.put("field3", 1.5);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals(5.5, value.get("subtract"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processMultiplyMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("field1", 7);
        message.put("field2", 0.5);
        message.put("field3", 1.5);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals(3.5, value.get("multiply"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processDivisionMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("field1", 7);
        message.put("field2", 0.5);
        message.put("field3", 1.5);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals(14.0, value.get("division"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("field1", 7);
        message.put("field2", 0.5);
        message.put("field3", 1.5);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process(null, message);
        assertNull(mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals(7.5, value.get("sum"));
        assertEquals(5.5, value.get("subtract"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void processNullKeyAndNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process(null, null);
        assertNull(mapMessage.key);
        assertNull(mapMessage.value);
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key", null);
        assertEquals("key",mapMessage.key);
        assertNull(mapMessage.value);
    }

    @Test
    public void processNullDimensionMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("field1", 7);
        message.put("field2", 0.5);
        message.put("field4", 1.5);

        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals(7.5, value.get("sum"));
        assertNull(value.get("subtract"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void truncateDoubleUnitTest() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream2");
        Function myFunc = functions.get("myMapper2");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof ArithmeticMapper);
        ArithmeticMapper myMapper = (ArithmeticMapper) myFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789);
        message.put("field1", 7.1234567);


        KeyValue<String, Map<String, Object>> mapMessage = myMapper.process("key1", message);
        assertEquals("key1", mapMessage.key);

        Map<String, Object> value = mapMessage.value;
        assertEquals(7.123, value.get("truncate"));
        assertNull(value.get("subtract"));
        assertEquals(123456789, value.get("timestamp"));
    }

    @Test
    public void toStringTest(){
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myMapper");

        assertNotNull(myFunc);
        assertEquals("{equations: [{dimensions=[field1, field2], equation=field1+field2, as=sum}," +
                " {dimensions=[field1, field3], equation=field1-field3, as=subtract}, {dimensions=[field1, field2], " +
                "equation=field1*field2, as=multiply}, {dimensions=[field1, field2], equation=field1/field2, as=division}, " +
                "{dimensions=[field1], equation=sqrt(field1), as=sqrt}]}", myFunc.toString());
    }

    @AfterClass
    public static void stopTest(){
        streamBuilder.close();
    }
}
