package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.base.utils.ConvertFrom;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.funcs.MapperFunction;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class FieldTypeConverterMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("field-type-converter-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("myFieldConverterMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof MapperFunction);
        FieldTypeConverterMapper fieldConverterMapper = (FieldTypeConverterMapper) myFunc;

        Map<String, Object> conversionA = new HashMap<>();
        conversionA.put("dimension", "dimension-A");
        conversionA.put("from", "number");
        conversionA.put("to", "string");

        Map<String, Object> conversionB = new HashMap<>();
        conversionB.put("dimension", "dimension-B");
        conversionB.put("from", "boolean");
        conversionB.put("to", "string");

        Map<String, Object> conversionC = new HashMap<>();
        conversionC.put("dimension", "dimension-C");
        conversionC.put("from", "string");
        conversionC.put("to", "string");

        Map<String, Object> conversionD = new HashMap<>();
        conversionD.put("dimension", "dimension-D");
        conversionD.put("from", "boolean");
        conversionD.put("to", "number");

        Map<String, Object> conversionE = new HashMap<>();
        conversionE.put("dimension", "dimension-E");
        conversionE.put("from", "string");
        conversionE.put("to", "number");

        Map<String, Object> conversionF = new HashMap<>();
        conversionF.put("dimension", "dimension-F");
        conversionF.put("from", "number");
        conversionF.put("to", "number");

        Map<String, Object> conversionG = new HashMap<>();
        conversionG.put("dimension", "dimension-G");
        conversionG.put("from", "number");
        conversionG.put("to", "boolean");

        Map<String, Object> conversionH = new HashMap<>();
        conversionH.put("dimension", "dimension-H");
        conversionH.put("from", "string");
        conversionH.put("to", "boolean");

        Map<String, Object> conversionI = new HashMap<>();
        conversionI.put("dimension", "dimension-I");
        conversionI.put("from", "boolean");
        conversionI.put("to", "boolean");

        List<Map<String, Object>> conversions = Arrays.asList(
                conversionA, conversionB, conversionC, conversionD, conversionE,
                conversionF, conversionG, conversionH, conversionI);

        assertEquals(conversions, fieldConverterMapper.conversions);
    }

    @Test
    public void processConversion() throws IOException, PlanBuilderException {
        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("field-type-converter-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);

        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myFieldConverterFunc = functions.get("myFieldConverterMapper");

        assertNotNull(myFieldConverterFunc);
        assertTrue(myFieldConverterFunc instanceof MapperFunction);
        FieldTypeConverterMapper fieldConverterMapper = (FieldTypeConverterMapper) myFieldConverterFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("dimension-A", 1.5);
        message.put("dimension-B", true);
        message.put("dimension-C", "foo");
        message.put("dimension-D", true);
        message.put("dimension-E", "1.57");
        message.put("dimension-F", -100);
        message.put("dimension-G", 7);
        message.put("dimension-H", "false");
        message.put("dimension-I", true);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("dimension-A", "1.5");
        expectedMessage.put("dimension-B", "true");
        expectedMessage.put("dimension-C", "foo");
        expectedMessage.put("dimension-D", 1);
        expectedMessage.put("dimension-E", 1.57);
        expectedMessage.put("dimension-F", -100);
        expectedMessage.put("dimension-G", true);
        expectedMessage.put("dimension-H", false);
        expectedMessage.put("dimension-I", true);

        KeyValue<String, Map<String, Object>> result1 = fieldConverterMapper.process("KEY", message);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage), result1);
    }

    @Test
    public void processNullMessage() throws IOException, PlanBuilderException {
        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("field-type-converter-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);

        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myFieldConverterFunc = functions.get("myFieldConverterMapper");

        assertNotNull(myFieldConverterFunc);
        assertTrue(myFieldConverterFunc instanceof MapperFunction);
        FieldTypeConverterMapper fieldConverterMapper = (FieldTypeConverterMapper) myFieldConverterFunc;

        Map<String, Object> message = null;

        Map<String, Object> expectedMessage = null;

        KeyValue<String, Map<String, Object>> result1 = fieldConverterMapper.process("KEY", message);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage), result1);
    }

    @Test
    public void processNullKeyAndMessage() throws IOException, PlanBuilderException {
        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("field-type-converter-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);

        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myFieldConverterFunc = functions.get("myFieldConverterMapper");

        assertNotNull(myFieldConverterFunc);
        assertTrue(myFieldConverterFunc instanceof MapperFunction);
        FieldTypeConverterMapper fieldConverterMapper = (FieldTypeConverterMapper) myFieldConverterFunc;

        Map<String, Object> message = null;

        Map<String, Object> expectedMessage = null;

        KeyValue<String, Map<String, Object>> result1 = fieldConverterMapper.process(null, message);
        streamBuilder2.close();
        assertEquals(new KeyValue<>(null, expectedMessage), result1);
    }

    @Test
    public void processNullKey() throws IOException, PlanBuilderException {
        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("field-type-converter-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);

        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myFieldConverterFunc = functions.get("myFieldConverterMapper");

        assertNotNull(myFieldConverterFunc);
        assertTrue(myFieldConverterFunc instanceof MapperFunction);
        FieldTypeConverterMapper fieldConverterMapper = (FieldTypeConverterMapper) myFieldConverterFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("dimension-A", 1.5);
        message.put("dimension-B", true);
        message.put("dimension-C", "foo");
        message.put("dimension-D", true);
        message.put("dimension-E", "1.57");
        message.put("dimension-F", -100);
        message.put("dimension-G", 7);
        message.put("dimension-H", "false");
        message.put("dimension-I", true);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("dimension-A", "1.5");
        expectedMessage.put("dimension-B", "true");
        expectedMessage.put("dimension-C", "foo");
        expectedMessage.put("dimension-D", 1);
        expectedMessage.put("dimension-E", 1.57);
        expectedMessage.put("dimension-F", -100);
        expectedMessage.put("dimension-G", true);
        expectedMessage.put("dimension-H", false);
        expectedMessage.put("dimension-I", true);

        KeyValue<String, Map<String, Object>> result1 = fieldConverterMapper.process(null, message);
        streamBuilder2.close();
        assertEquals(new KeyValue<>(null, expectedMessage), result1);
    }

    @Test
    public void processConversionWithNewDimensions() throws IOException, PlanBuilderException {
        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("field-type-converter-with-new-fields-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);

        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myFieldConverterFunc = functions.get("myFieldConverterMapper");

        assertNotNull(myFieldConverterFunc);
        assertTrue(myFieldConverterFunc instanceof MapperFunction);
        FieldTypeConverterMapper fieldConverterMapper = (FieldTypeConverterMapper) myFieldConverterFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("dimension-A", 1.5);
        message.put("dimension-B", true);
        message.put("dimension-C", "foo");
        message.put("dimension-D", true);
        message.put("dimension-E", "1.57");
        message.put("dimension-F", -100);
        message.put("dimension-G", 7);
        message.put("dimension-H", "false");
        message.put("dimension-I", true);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("dimension-A", 1.5);
        expectedMessage.put("dimension-B", true);
        expectedMessage.put("dimension-C", "foo");
        expectedMessage.put("dimension-D", true);
        expectedMessage.put("dimension-E", "1.57");
        expectedMessage.put("dimension-F", -100);
        expectedMessage.put("dimension-G", 7);
        expectedMessage.put("dimension-H", "false");
        expectedMessage.put("dimension-I", true);
        expectedMessage.put("num2str", "1.5");
        expectedMessage.put("bool2str", "true");
        expectedMessage.put("str2str", "foo");
        expectedMessage.put("bool2num", 1);
        expectedMessage.put("str2num", 1.57);
        expectedMessage.put("num2num", -100);
        expectedMessage.put("num2bool", true);
        expectedMessage.put("str2bool", false);
        expectedMessage.put("bool2bool", true);

        KeyValue<String, Map<String, Object>> result1 = fieldConverterMapper.process("KEY", message);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage), result1);
    }

    @Test
    public void processConversionWithNullDimensions() throws IOException, PlanBuilderException {
        StreamBuilder streamBuilder2 = new StreamBuilder(config, null);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("field-type-converter-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder2.builder(model);

        Map<String, Function> functions = streamBuilder2.getFunctions("stream1");
        Function myFieldConverterFunc = functions.get("myFieldConverterMapper");

        assertNotNull(myFieldConverterFunc);
        assertTrue(myFieldConverterFunc instanceof MapperFunction);
        FieldTypeConverterMapper fieldConverterMapper = (FieldTypeConverterMapper) myFieldConverterFunc;

        Map<String, Object> message = new HashMap<>();
        message.put("dimension-J", 1.5);
        message.put("dimension-K", true);
        message.put("dimension-L", "foo");
        message.put("dimension-M", true);
        message.put("dimension-N", "1.57");
        message.put("dimension-O", -100);
        message.put("dimension-P", 7);
        message.put("dimension-Q", "false");
        message.put("dimension-R", true);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put("dimension-J", 1.5);
        expectedMessage.put("dimension-K", true);
        expectedMessage.put("dimension-L", "foo");
        expectedMessage.put("dimension-M", true);
        expectedMessage.put("dimension-N", "1.57");
        expectedMessage.put("dimension-O", -100);
        expectedMessage.put("dimension-P", 7);
        expectedMessage.put("dimension-Q", "false");
        expectedMessage.put("dimension-R", true);

        KeyValue<String, Map<String, Object>> result1 = fieldConverterMapper.process("KEY", message);
        streamBuilder2.close();
        assertEquals(new KeyValue<>("KEY", expectedMessage), result1);
    }

    @Test
    public void numberToStringConversionShouldWorks() throws ParseException {
        Assert.assertEquals("1", ConvertFrom.NUMBER.toString(1));
        assertEquals("1.0", ConvertFrom.NUMBER.toString(1.0));
        assertEquals("-1", ConvertFrom.NUMBER.toString(-1));
        assertEquals("-1.0", ConvertFrom.NUMBER.toString(-1.0));
    }

    @Test
    public void numberToBooleanConversionShouldWorks() {
        assertEquals(true, ConvertFrom.NUMBER.toBoolean(1));
        assertEquals(true, ConvertFrom.NUMBER.toBoolean(1.0));
        assertEquals(false, ConvertFrom.NUMBER.toBoolean(-1));
        assertEquals(false, ConvertFrom.NUMBER.toBoolean(-1.0));
        assertEquals(false, ConvertFrom.NUMBER.toBoolean(0));
    }

    @Test
    public void numberToNumberConversionShouldWorks() throws ParseException {
        assertEquals(1, ConvertFrom.NUMBER.toNumber(1));
        assertEquals(1L, ConvertFrom.NUMBER.toNumber(1L));
        assertEquals(1.0, ConvertFrom.NUMBER.toNumber(1.0));
        assertEquals(1.0F, ConvertFrom.NUMBER.toNumber(1.0F));
        assertEquals(-1, ConvertFrom.NUMBER.toNumber(-1));
        assertEquals(-1L, ConvertFrom.NUMBER.toNumber(-1L));
        assertEquals(-1.0, ConvertFrom.NUMBER.toNumber(-1.0));
        assertEquals(-1.0F, ConvertFrom.NUMBER.toNumber(-1.0F));
    }

    @Test
    public void booleanToStringConversionShouldWorks() throws ParseException {
        assertEquals("true", ConvertFrom.BOOLEAN.toString(true));
        assertEquals("false", ConvertFrom.BOOLEAN.toString(false));
    }

    @Test
    public void booleanToBooleanConversionShouldWorks() {
        assertEquals(true, ConvertFrom.BOOLEAN.toBoolean(true));
        assertEquals(false, ConvertFrom.BOOLEAN.toBoolean(false));
    }

    @Test
    public void booleanToNumberConversionShouldWorks() throws ParseException {
        assertEquals(1, ConvertFrom.BOOLEAN.toNumber(true));
        assertEquals(0, ConvertFrom.BOOLEAN.toNumber(false));
    }

    @Test
    public void stringToStringConversionShouldWorks() throws ParseException {
        assertEquals("foo", ConvertFrom.STRING.toString("foo"));
        assertEquals("test", ConvertFrom.STRING.toString("test"));
    }

    @Test
    public void stringToBooleanConversionShouldWorks() {
        assertEquals(true, ConvertFrom.STRING.toBoolean("true"));
        assertEquals(false, ConvertFrom.STRING.toBoolean("false"));
    }

    @Test
    public void stringToNumberConversionShouldWorks() throws ParseException {
        assertEquals(1L, ConvertFrom.STRING.toNumber("1"));
        assertEquals(1.0, ConvertFrom.STRING.toNumber("1.0").doubleValue(), 0);
        assertEquals(-1L, ConvertFrom.STRING.toNumber("-1"));
        assertEquals(-1.0, ConvertFrom.STRING.toNumber("-1.0").doubleValue(), 0);
    }

    @AfterClass
    public static void stop() {
        streamBuilder.close();
    }

}
