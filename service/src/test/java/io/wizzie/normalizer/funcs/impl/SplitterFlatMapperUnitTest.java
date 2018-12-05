package io.wizzie.normalizer.funcs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.FlatMapperFunction;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class SplitterFlatMapperUnitTest {
    private long secs(DateTime date) {
        return (date.getMillis() / 1000);
    }

    private DateTime currentTime = new DateTime();
    private DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    static Config config = new Config();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");
    }

    private static StreamBuilder streamBuilder = new StreamBuilder(config, null);

    @BeforeClass
    public static void initTest() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("splitter-flat-mapper.json").getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(file, PlanModel.class);
        streamBuilder.builder(model);
    }

    @Test
    public void building() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        assertNotNull(myFunc);
        assertTrue(myFunc instanceof FlatMapperFunction);
        SplitterFlatMapper mySplitterFlatMapper = (SplitterFlatMapper) myFunc;

        assertEquals(Arrays.asList("A", "B", "C", "D"), mySplitterFlatMapper.splitter.dimensions);
    }

    @Test
    public void withoutFirstSwitched() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        Map<String, Object> message = new HashMap<>();
        message.put("timestamp", secs(currentTime));
        message.put("A", 999);
        KeyValue<String, Map<String, Object>> expectedMessage = new KeyValue<>("key1", message);

        List<KeyValue<String, Map<String, Object>>> expected = Collections.singletonList(expectedMessage);
        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFunc.process("key1", message);
        assertEquals(expected, result);
    }

    @Test
    public void withoutTimestamp() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        Map<String, Object> message = new HashMap<>();
        message.put("A", 999);
        message.put("B", 99);

        KeyValue<String, Map<String, Object>> expectedMessage = new KeyValue<>("key1", message);

        List<KeyValue<String, Map<String, Object>>> expected = Collections.singletonList(expectedMessage);

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFunc.process("key1", message);
        message.put("timestamp", secs(currentTime));

        assertEquals(expected, result);
    }

    @Test
    public void lessThanAMinute() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:42");

        message.put("timestamp", secs(timestampDate));
        message.put("last_timestamp", secs(firstSwitchDate));
        message.put("C", 999L);
        message.put("D", 99L);

        Map<String, Object> expected = new HashMap<>();
        expected.put("timestamp", secs(firstSwitchDate));
        expected.put("C", 999L);
        expected.put("D", 99L);

        KeyValue<String, Object> expectedMessage = new KeyValue<>("key1", expected);

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFunc.process("key1", message);

        assertEquals(expectedMessage, result.get(0));
    }

    @Test
    public void processNullMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        Map<String, Object> message = null;

        Map<String, Object> expected = null;

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFunc.process("key1", message);

        assertEquals(null, result);
    }

    @Test
    public void processNullKeyAndMessage() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        Map<String, Object> message = null;

        Map<String, Object> expected = null;

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFunc.process(null, message);

        assertEquals(null, result);
    }

    @Test
    public void processNullKey() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:42");

        message.put("timestamp", secs(timestampDate));
        message.put("last_timestamp", secs(firstSwitchDate));
        message.put("C", 999L);
        message.put("D", 99L);

        Map<String, Object> expected = new HashMap<>();
        expected.put("timestamp", secs(firstSwitchDate));
        expected.put("C", 999L);
        expected.put("D", 99L);

        KeyValue<String, Object> expectedMessage = new KeyValue<>(null, expected);

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFunc.process(null, message);

        assertEquals(expectedMessage, result.get(0));
    }

    @Test
    public void higherThanAMinute() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:13:42");

        message.put("timestamp", secs(timestampDate));
        message.put("last_timestamp", secs(firstSwitchDate));
        message.put("A", 999);
        message.put("D", 99);

        List<KeyValue<String, Map<String, Object>>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:12");
        expected.put("timestamp", secs(pktTime));
        expected.put("A", 228L);
        expected.put("D", 22L);
        expectedPackets.add(new KeyValue<>("key1", expected));

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:11:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("A", 285L);
        expected.put("D", 28L);
        expectedPackets.add(new KeyValue<>("key1", expected));

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:12:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("A", 285L);
        expected.put("D", 28L);
        expectedPackets.add(new KeyValue<>("key1", expected));

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:13:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("A", 201L);
        expected.put("D", 21L);
        expectedPackets.add(new KeyValue<>("key1", expected));

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFunc.process("key1", message);

        assertEquals(expectedPackets, result);
    }

    @Test
    public void basicSpli() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2016-04-06 10:01:26");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2016-04-06 10:06:26");

        message.put("timestamp", secs(timestampDate));
        message.put("last_timestamp", secs(firstSwitchDate));
        message.put("B", 316402980L);
        message.put("D", 316402980L);

        List<KeyValue<String, Map<String, Object>>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-04-06 10:01:26");
        expected.put("timestamp", secs(pktTime));
        expected.put("B", 35859004L);
        expected.put("D", 35859004L);
        expectedPackets.add(new KeyValue<>("key1", expected));

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-04-06 10:02:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("B", 63280596L);
        expected.put("D", 63280596L);
        expectedPackets.add(new KeyValue<>("key1", expected));

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-04-06 10:03:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("B", 63280596L);
        expected.put("D", 63280596L);
        expectedPackets.add(new KeyValue<>("key1", expected));

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-04-06 10:04:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("B", 63280596L);
        expected.put("D", 63280596L);
        expectedPackets.add(new KeyValue<>("key1", expected));

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-04-06 10:05:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("B", 63280596L);
        expected.put("D", 63280596L);
        expectedPackets.add(new KeyValue<>("key1", expected));

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-04-06 10:06:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("B", 27421592L);
        expected.put("D", 27421592L);
        expectedPackets.add(new KeyValue<>("key1", expected));


        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFunc.process("key1", message);
        assertEquals(expectedPackets, result);
    }


    @Test
    public void numberFormatExceptions() {
        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function myFunc = functions.get("mySplitterFlatMapper");

        Map<String, Object> message = new HashMap<>();

        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:13:42");

        message.put("timestamp", secs(timestampDate));
        message.put("A", 2000000000.0);
        message.put("B", 15625000);

        List<KeyValue<String, Map<String, Object>>> result = (List<KeyValue<String, Map<String, Object>>>) myFunc.process("key1", message);
        assertTrue(result.isEmpty());
    }
}
