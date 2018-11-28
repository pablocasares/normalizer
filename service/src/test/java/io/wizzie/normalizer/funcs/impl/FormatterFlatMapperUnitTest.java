package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class FormatterFlatMapperUnitTest {

    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);
    private static FormatterFlatMapper formatterFlatMapper;
    private static FormatterFlatMapper formatterFlatMapperWithPass;

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ObjectMapper objectMapper = new ObjectMapper();

        File file = new File(classLoader.getResource("formatter-flat-mapper.json").getFile());
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
        Map<String, Function> functionsMap = streamBuilder.getFunctions("stream1");
        formatterFlatMapper = (FormatterFlatMapper) functionsMap.get("myFormatterFlatMapper");

        File file1 = new File(classLoader.getResource("formatter-flat-mapper-with-pass.json").getFile());
        PlanModel model1 = objectMapper.readValue(file1, PlanModel.class);
        streamBuilder.builder(model1);
        Map<String, Function> functionsMap1 = streamBuilder.getFunctions("stream1");
        formatterFlatMapperWithPass = (FormatterFlatMapper) functionsMap1.get("myFormatterFlatMapperWithPass");
    }

    @Test
    public void building() {
        assertNotNull(formatterFlatMapper);
        assertEquals(3, formatterFlatMapper.filters.size());
        assertTrue(formatterFlatMapper.filters.containsKey("typeAFilter"));
        assertTrue(formatterFlatMapper.filters.containsKey("typeBFilter"));
        assertTrue(formatterFlatMapper.filters.get("typeAFilter") instanceof FieldFilter);
        assertTrue(formatterFlatMapper.filters.get("typeBFilter") instanceof FieldFilter);
        assertEquals(3, formatterFlatMapper.generators.size());

        List<Map<String, Object>> generators = formatterFlatMapper.generators;
        List<Map<String, Object>> generator1 = (List<Map<String, Object>>) generators.get(0).get("definitions");

        assertEquals(4, generator1.size());

        List<Map<String, Object>> generator2 = (List<Map<String, Object>>) generators.get(1).get("definitions");

        assertEquals(2, generator2.size());
    }

    @Test
    public void processCompleteFilteredMessageCorrectly() {
        List<KeyValue<String, Map<String, Object>>> expectedList = new ArrayList<>();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("type", "A");
        message1.put("Q", true);
        message1.put("V", "VALUE-V");
        message1.put("W", "VALUE-W");
        message1.put("X", "VALUE-X");
        message1.put("Y", "VALUE-Y");

        String key1 = "KEY1";

        List<KeyValue<String, Map<String, Object>>> results = (List<KeyValue<String, Map<String, Object>>>) formatterFlatMapper.process(key1, message1);

        assertEquals(4, results.size());

        Map<String, Object> expectedMessage1_1 = new HashMap<>();
        expectedMessage1_1.put("timestamp", 123456789L);
        expectedMessage1_1.put("Q", true);
        expectedMessage1_1.put("type", "VALUE-I");
        expectedMessage1_1.put("enable", true);
        expectedMessage1_1.put("value", "VALUE-V");

        KeyValue<String, Map<String, Object>> expectedKv1_1 = new KeyValue<>(key1, expectedMessage1_1);

        expectedList.add(expectedKv1_1);

        Map<String, Object> expectedMessage1_2 = new HashMap<>();
        expectedMessage1_2.put("timestamp", 123456789L);
        expectedMessage1_2.put("Q", true);
        expectedMessage1_2.put("type", "VALUE-J");
        expectedMessage1_2.put("enable", false);
        expectedMessage1_2.put("value", "VALUE-W");

        KeyValue<String, Map<String, Object>> expectedKv1_2 = new KeyValue<>(key1, expectedMessage1_2);

        expectedList.add(expectedKv1_2);

        Map<String, Object> expectedMessage1_3 = new HashMap<>();
        expectedMessage1_3.put("timestamp", 123456789L);
        expectedMessage1_3.put("Q", true);
        expectedMessage1_3.put("type", "VALUE-K");
        expectedMessage1_3.put("enable", true);
        expectedMessage1_3.put("value", "VALUE-X");

        KeyValue<String, Map<String, Object>> expectedKv1_3 = new KeyValue<>(key1, expectedMessage1_3);

        expectedList.add(expectedKv1_3);

        Map<String, Object> expectedMessage1_4 = new HashMap<>();
        expectedMessage1_4.put("timestamp", 123456789L);
        expectedMessage1_4.put("Q", true);
        expectedMessage1_4.put("type", "VALUE-L");
        expectedMessage1_4.put("enable", false);
        expectedMessage1_4.put("value", "VALUE-Y");

        KeyValue<String, Map<String, Object>> expectedKv1_4 = new KeyValue<>(key1, expectedMessage1_4);

        expectedList.add(expectedKv1_4);

        assertEquals(expectedList, results);

        expectedList.clear();

        Map<String, Object> message2 = new HashMap<>();
        message2.put("timestamp", 123456789L);
        message2.put("type", "B");
        message2.put("Q", true);
        message2.put("X", "VALUE-X");
        message2.put("Y", "VALUE-Y");

        String key2 = "KEY2";

        results = (List<KeyValue<String, Map<String, Object>>>) formatterFlatMapper.process(key2, message2);

        assertEquals(2, results.size());

        Map<String, Object> expectedMessage2_1 = new HashMap<>();
        expectedMessage2_1.put("timestamp", 123456789L);
        expectedMessage2_1.put("Q", true);
        expectedMessage2_1.put("type", "VALUE-M");
        expectedMessage2_1.put("enable", false);
        expectedMessage2_1.put("value", "VALUE-X");

        KeyValue<String, Map<String, Object>> expectedKv2_1 = new KeyValue<>(key2, expectedMessage2_1);

        expectedList.add(expectedKv2_1);

        Map<String, Object> expectedMessage2_2 = new HashMap<>();
        expectedMessage2_2.put("timestamp", 123456789L);
        expectedMessage2_2.put("Q", true);
        expectedMessage2_2.put("type", "VALUE-N");
        expectedMessage2_2.put("enable", true);
        expectedMessage2_2.put("value", "VALUE-Y");

        KeyValue<String, Map<String, Object>> expectedKv2_2 = new KeyValue<>(key2, expectedMessage2_2);

        expectedList.add(expectedKv2_2);

        assertEquals(expectedList, results);
    }

    @Test
    public void processCompleteFilteredMATCHMessageCorrectly() {
        List<KeyValue<String, Map<String, Object>>> expectedList = new ArrayList<>();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("timestamp", 123456789L);
        message1.put("type", "X");
        message1.put("Q", true);
        message1.put("V", "VALUE-V");
        message1.put("W", "VALUE-W");
        message1.put("X", "VALUE-X");
        message1.put("Y", "VALUE-Y");
        message1.put("match", "some");

        String key1 = "KEY1";

        List<KeyValue<String, Map<String, Object>>> results = (List<KeyValue<String, Map<String, Object>>>) formatterFlatMapper.process(key1, message1);

        assertEquals(1, results.size());

        Map<String, Object> expectedMessage1_1 = new HashMap<>();
        expectedMessage1_1.put("timestamp", 123456789L);
        expectedMessage1_1.put("Q", true);
        expectedMessage1_1.put("MATCH", "MATCH");

        expectedList.add(new KeyValue<>(key1, expectedMessage1_1));

        assertEquals(expectedList, results);
    }

    @Test
    public void notProcessFilteredMessage() {
        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("type", "C");
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");

        String key = "KEY1";

        List<KeyValue<String, Map<String, Object>>> results = (List<KeyValue<String, Map<String, Object>>>) formatterFlatMapper.process(key, message);

        assertEquals(0, results.size());

        assertEquals(Collections.EMPTY_LIST, results);
    }

    @Test
    public void notProcessFilteredMessageWithPass(){
        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("type", "C");
        message.put("A", "VALUE-A");
        message.put("B", "VALUE-B");

        String key = "KEY1";

        List<KeyValue<String, Map<String, Object>>> results = (List<KeyValue<String, Map<String, Object>>>) formatterFlatMapperWithPass.process(key, message);

        assertEquals(key, results.get(0).key);
        assertEquals(message, results.get(0).value);
    }

    @Test
    public void processPartialMessageCorrectly() {
        List<KeyValue<String, Map<String, Object>>> expectedList = new ArrayList<>();

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("type", "A");
        message.put("Q", true);
        message.put("V", "VALUE-V");
        message.put("Y", "VALUE-Y");

        String key = "KEY0";

        List<KeyValue<String, Map<String, Object>>> results = (List<KeyValue<String, Map<String, Object>>>) formatterFlatMapper.process(key, message);

        assertEquals(2, results.size());

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 123456789L);
        expectedMessage1.put("Q", true);
        expectedMessage1.put("type", "VALUE-I");
        expectedMessage1.put("enable", true);
        expectedMessage1.put("value", "VALUE-V");

        KeyValue<String, Map<String, Object>> expectedKv1 = new KeyValue<>(key, expectedMessage1);

        expectedList.add(expectedKv1);

        Map<String, Object> expectedMessage2 = new HashMap<>();
        expectedMessage2.put("timestamp", 123456789L);
        expectedMessage2.put("Q", true);
        expectedMessage2.put("type", "VALUE-L");
        expectedMessage2.put("enable", false);
        expectedMessage2.put("value", "VALUE-Y");

        KeyValue<String, Map<String, Object>> expectedKv2 = new KeyValue<>(key, expectedMessage2);

        expectedList.add(expectedKv2);

        assertEquals(expectedList, results);
    }

    @Test
    public void notProcessNullValues() {
        List<KeyValue<String, Map<String, Object>>> expectedList = new ArrayList<>();

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", 123456789L);
        message.put("type", "A");
        message.put("V", "VALUE-V");
        message.put("Y", null);
        message.put(null, null);

        String key = "KEY0";

        List<KeyValue<String, Map<String, Object>>> results = (List<KeyValue<String, Map<String, Object>>>) formatterFlatMapper.process(key, message);

        assertEquals(1, results.size());

        Map<String, Object> expectedMessage1 = new HashMap<>();
        expectedMessage1.put("timestamp", 123456789L);
        expectedMessage1.put("type", "VALUE-I");
        expectedMessage1.put("enable", true);
        expectedMessage1.put("value", "VALUE-V");

        KeyValue<String, Map<String, Object>> expectedKv1 = new KeyValue<>(key, expectedMessage1);

        expectedList.add(expectedKv1);

        assertEquals(expectedList, results);
    }

    @Test
    public void notProcessNullMessages() {
        String key = "KEY";

        List<KeyValue<String, Map<String, Object>>> results = (List<KeyValue<String, Map<String, Object>>>) formatterFlatMapper.process(key, null);

        assertEquals(0, results.size());
    }

    @Test
    public void notProcessNullKeysAndMessages() {
        String key = null;

        List<KeyValue<String, Map<String, Object>>> results = (List<KeyValue<String, Map<String, Object>>>) formatterFlatMapper.process(key, null);

        assertEquals(0, results.size());
    }

}
