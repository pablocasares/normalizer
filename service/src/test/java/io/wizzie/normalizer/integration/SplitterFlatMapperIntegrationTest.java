package io.wizzie.normalizer.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.FlatMapperFunction;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.model.PlanModel;
import io.wizzie.normalizer.serializers.JsonDeserializer;
import io.wizzie.normalizer.serializers.JsonSerde;
import io.wizzie.normalizer.serializers.JsonSerializer;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class SplitterFlatMapperIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;
    private static DateTimeFormatter formatter;

    public long secs(DateTime date) {
        return (date.getMillis() / 1000);
    }

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic("topic1", 2, REPLICATION_FACTOR);

        // sinks
        CLUSTER.createTopic("output1", 4, REPLICATION_FACTOR);

        formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    }

    @Test
    public void shouldWorkWithKafkaTest() throws InterruptedException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("splitter-flat-mapper-integration.json").getFile());

        Properties streamsConfiguration = new Properties();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        DateTime firstswitched1 = formatter.withZoneUTC().parseDateTime("2016-09-07 11:10:00");
        DateTime timestamp1 = formatter.withZoneUTC().parseDateTime("2016-09-07 11:14:00");

        DateTime firstswitched2 = formatter.withZoneUTC().parseDateTime("2016-09-07 11:17:21");
        DateTime timestamp2 = formatter.withZoneUTC().parseDateTime("2016-09-07 11:21:37");

        Config config = new Config();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");

        StreamBuilder streamBuilder = new StreamBuilder(config, null);

        KafkaStreams streams = null;
        try {
            streams = new KafkaStreams(streamBuilder.builder(model).build(), streamsConfiguration);
        } catch (PlanBuilderException e) {
            fail("Exception : " + e.getMessage());
        }

        Map<String, Function> functions = streamBuilder.getFunctions("stream1");
        Function splitterFlatMapper = functions.get("mySplitterFlatMapper");

        assertNotNull(splitterFlatMapper);
        assertTrue(splitterFlatMapper instanceof FlatMapperFunction);

        streams.start();

        StringBuilder message1 = new StringBuilder();
        StringBuilder message2 = new StringBuilder();

        message1.append("{")
                .append("\"last_timestamp\": ")
                .append((int) secs(firstswitched1))
                .append(", ")
                .append("\"timestamp\": ")
                .append((int) secs(timestamp1))
                .append(", ")
                .append("\"A\" : 7500, ")
                .append("\"D\": 10000")
                .append("}");

        message2.append("{")
                .append("\"last_timestamp\": ")
                .append((int) secs(firstswitched2))
                .append(", ")
                .append("\"timestamp\": ")
                .append((int) secs(timestamp2))
                .append(", ")
                .append("\"B\" : 5102, ")
                .append("\"C\": 4030")
                .append("}");

        KeyValue<String, Map<String, Object>> kvStream1 = null;
        KeyValue<String, Map<String, Object>> kvStream2 = null;

        try {
            kvStream1 = new KeyValue<>("key1", objectMapper.readValue(message1.toString(), Map.class));
            kvStream2 = new KeyValue<>("key2", objectMapper.readValue(message2.toString(), Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously("topic1", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
            IntegrationTestUtils.produceKeyValuesSynchronously("topic1", Collections.singletonList(kvStream2), producerConfig, MOCK_TIME);

        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Properties consumerConfigB = new Properties();
        consumerConfigB.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigB.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-B");
        consumerConfigB.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigB.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        List<KeyValue<String, Map<String, Object>>> expectedMessages = new ArrayList<>();

        Map<String, Object> expectedMessage = new HashMap<>();
        DateTime pktTime;

        // Expected message 1
        pktTime = formatter.withZoneUTC().parseDateTime("2016-09-07 11:10:00");
        expectedMessage.put("timestamp", (int) secs(pktTime));
        expectedMessage.put("A", 1875);
        expectedMessage.put("D", 2500);
        expectedMessages.add(new KeyValue<>("key1", expectedMessage));

        expectedMessage = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-09-07 11:11:00");
        expectedMessage.put("timestamp", (int) secs(pktTime));
        expectedMessage.put("A", 1875);
        expectedMessage.put("D", 2500);
        expectedMessages.add(new KeyValue<>("key1", expectedMessage));

        expectedMessage = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-09-07 11:12:00");
        expectedMessage.put("timestamp", (int) secs(pktTime));
        expectedMessage.put("A", 1875);
        expectedMessage.put("D", 2500);
        expectedMessages.add(new KeyValue<>("key1", expectedMessage));

        expectedMessage = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-09-07 11:13:00");
        expectedMessage.put("timestamp", (int) secs(pktTime));
        expectedMessage.put("A", 1875);
        expectedMessage.put("D", 2500);
        expectedMessages.add(new KeyValue<>("key1", expectedMessage));


        // Expected message 2
        expectedMessage = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-09-07 11:17:21");
        expectedMessage.put("timestamp", (int) secs(pktTime));
        expectedMessage.put("B", 777);
        expectedMessage.put("C", 613);
        expectedMessages.add(new KeyValue<>("key2", expectedMessage));

        expectedMessage = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-09-07 11:18:00");
        expectedMessage.put("timestamp", (int) secs(pktTime));
        expectedMessage.put("B", 1195);
        expectedMessage.put("C", 944);
        expectedMessages.add(new KeyValue<>("key2", expectedMessage));

        expectedMessage = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-09-07 11:19:00");
        expectedMessage.put("timestamp", (int) secs(pktTime));
        expectedMessage.put("B", 1195);
        expectedMessage.put("C", 944);
        expectedMessages.add(new KeyValue<>("key2", expectedMessage));

        expectedMessage = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-09-07 11:20:00");
        expectedMessage.put("timestamp", (int) secs(pktTime));
        expectedMessage.put("B", 1195);
        expectedMessage.put("C", 944);
        expectedMessages.add(new KeyValue<>("key2", expectedMessage));

        expectedMessage = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2016-09-07 11:21:00");
        expectedMessage.put("timestamp", (int) secs(pktTime));
        expectedMessage.put("B", 740);
        expectedMessage.put("C", 585);
        expectedMessages.add(new KeyValue<>("key2", expectedMessage));

        List<KeyValue<String, Map>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output1", 2);

        assertTrue(expectedMessages.containsAll(result));

        streams.close();
        streamBuilder.close();

    }
}
