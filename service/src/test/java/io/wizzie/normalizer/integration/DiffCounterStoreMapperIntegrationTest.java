package io.wizzie.normalizer.integration;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.funcs.Function;
import io.wizzie.normalizer.funcs.MapperStoreFunction;
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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class DiffCounterStoreMapperIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_TOPIC = "input1";
    private static final String OUTPUT_TOPIC = "output1";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic(INPUT_TOPIC, 2, REPLICATION_FACTOR);

        // sinks
        CLUSTER.createTopic(OUTPUT_TOPIC, 4, REPLICATION_FACTOR);
    }

    @Test
    public void diffCounterStoreMapperShouldWork() throws InterruptedException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("diff-counter-store-mapper-integration.json").getFile());

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
        Function diffCounterMapperFunction = functions.get("diffCounterMapper");

        assertNotNull(diffCounterMapperFunction);
        assertTrue(diffCounterMapperFunction instanceof MapperStoreFunction);

        Set<String> stores = streamBuilder.usedStores();

        assertEquals(1, stores.size(), 0);
        assertTrue(stores.contains("app-id-1_counter-store"));

        streams.start();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("gateway", "gateway_1");
        message1.put("interface", "interface_1");
        message1.put("A", "VALUE-A");
        message1.put("B", "VALUE-B");
        message1.put("C", 1234567890L);
        message1.put("X", 1000L);
        message1.put("time", 1122334455L);

        KeyValue<String, Map<String, Object>> kvStream1 = new KeyValue<>("KEY_A", message1);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("gateway", "gateway_1");
        message2.put("interface", "interface_1");
        message2.put("A", "VALUE-A");
        message2.put("B", "VALUE-B");
        message2.put("C", 1234567890L);
        message2.put("X", 1000L);
        message2.put("time", 1122334655L);

        KeyValue<String, Map<String, Object>> kvStream2 = new KeyValue<>("KEY_A", message2);

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        try {

            IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
            IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Collections.singletonList(kvStream2), producerConfig, MOCK_TIME);

        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-diff-consumer");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Map<String, Object> expectedData1 = new HashMap<>();
        expectedData1.put("gateway", "gateway_1");
        expectedData1.put("interface", "interface_1");
        expectedData1.put("A", "VALUE-A");
        expectedData1.put("B", "VALUE-B");
        expectedData1.put("C", 1234567890);
        expectedData1.put("time", 1122334455);

        Map<String, Object> expectedData2 = new HashMap<>();
        expectedData2.put("gateway", "gateway_1");
        expectedData2.put("interface", "interface_1");
        expectedData2.put("A", "VALUE-A");
        expectedData2.put("B", "VALUE-B");
        expectedData2.put("C", 1234567890);
        expectedData2.put("X", 0);
        expectedData2.put("time", 1122334655);
        expectedData2.put("last_timestamp", 1122334455);


        KeyValue<String, Map<String, Object>> expectedDataKv1 = new KeyValue<>("KEY_A", expectedData1);

        KeyValue<String, Map<String, Object>> expectedDataKv2 = new KeyValue<>("KEY_A", expectedData2);

        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, OUTPUT_TOPIC, 2);

        assertEquals(Arrays.asList(expectedDataKv1, expectedDataKv2), receivedMessagesFromOutput1);

        streams.close();
        streamBuilder.close();
    }
}

