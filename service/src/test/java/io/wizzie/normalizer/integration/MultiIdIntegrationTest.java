package io.wizzie.normalizer.integration;


import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.base.builder.config.ConfigProperties;
import io.wizzie.normalizer.builder.Builder;
import io.wizzie.normalizer.serializers.JsonDeserializer;
import io.wizzie.normalizer.serializers.JsonSerde;
import io.wizzie.normalizer.serializers.JsonSerializer;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;

public class MultiIdIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_TOPIC_A = "myapp_A_input1";
    private static final String INPUT_TOPIC_B = "myapp_B_input1";

    private static final String OUTPUT_TOPIC1_A = "myapp_A_output1";

    private static final String OUTPUT_TOPIC1_B = "myapp_B_output1";

    private static final String BOOTSTRAP_TOPIC = "__normalizer_bootstrap";

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_TOPIC_A, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(INPUT_TOPIC_B, 2, REPLICATION_FACTOR);

        CLUSTER.createTopic(OUTPUT_TOPIC1_A, 2, REPLICATION_FACTOR);

        CLUSTER.createTopic(OUTPUT_TOPIC1_B, 2, REPLICATION_FACTOR);

        CLUSTER.createTopic(BOOTSTRAP_TOPIC, 1, REPLICATION_FACTOR);


        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void multiIdShouldWork() throws Exception {
        Map<String, Object> streamsConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Config config_A = new Config(streamsConfiguration);
        config_A.put("file.bootstrapper.path", Thread.currentThread().getContextClassLoader().getResource("multi-id-integration-test.json").getFile());
        config_A.put(ConfigProperties.BOOTSTRAPPER_CLASSNAME, "io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper");
        config_A.put("metric.enabled", false);
        config_A.put("multi.id", true);
        config_A.put("application.id", "myapp_A");

        Config config_B = config_A.clone();
        config_B.put("application.id", "myapp_B");

        Builder builder_A = new Builder(config_A);

        Builder builder_B = new Builder(config_B);

        Map<String, Object> b1_A = new HashMap<>();
        b1_A.put("C", 6000L);

        Map<String, Object> a1_A = new HashMap<>();
        a1_A.put("B", b1_A);

        Map<String, Object> message1_A = new HashMap<>();
        message1_A.put("A", a1_A);

        message1_A.put("timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> kvStream1_A = new KeyValue<>("KEY_A", message1_A);

        Map<String, Object> b2_A = new HashMap<>();
        b2_A.put("C", 9000L);

        Map<String, Object> a2_A = new HashMap<>();
        a2_A.put("B", b2_A);

        Map<String, Object> message2_A = new HashMap<>();
        message2_A.put("A", a2_A);

        message2_A.put("timestamp", 1122334655L);

        KeyValue<String, Map<String, Object>> kvStream2_A = new KeyValue<>("KEY_A", message2_A);

        Map<String, Object> b1_B = new HashMap<>();
        b1_B.put("C", 6000L);

        Map<String, Object> a1_B = new HashMap<>();
        a1_B.put("B", b1_B);

        Map<String, Object> message1_B = new HashMap<>();
        message1_B.put("A", a1_B);

        message1_B.put("timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> kvStream1_B = new KeyValue<>("KEY_B", message1_B);

        Map<String, Object> b2_B = new HashMap<>();
        b2_B.put("C", 9000L);

        Map<String, Object> a2_B = new HashMap<>();
        a2_B.put("B", b2_B);

        Map<String, Object> message2_B = new HashMap<>();
        message2_B.put("A", a2_B);

        message2_B.put("timestamp", 1122334655L);

        KeyValue<String, Map<String, Object>> kvStream2_B = new KeyValue<>("KEY_B", message2_B);


        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC_A, Arrays.asList(kvStream1_A, kvStream2_A), producerConfig, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC_B, Arrays.asList(kvStream1_B, kvStream2_B), producerConfig, MOCK_TIME);


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("X", 6000);
        expectedData.put("timestamp", 1122334455);

        Map<String, Object> expectedData2 = new HashMap<>();
        expectedData2.put("X", 9000);
        expectedData2.put("timestamp", 1122334655);

        List<KeyValue> expectedResult_A = Arrays.asList(new KeyValue("KEY_A", expectedData), new KeyValue("KEY_A", expectedData2));

        List<KeyValue<String, Map>> receivedMessagesFromOutput_A = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC1_A, 1);

        assertEquals(expectedResult_A, receivedMessagesFromOutput_A);

        List<KeyValue> expectedResult_B = Arrays.asList(new KeyValue("KEY_B", expectedData), new KeyValue("KEY_B", expectedData2));

        List<KeyValue<String, Map>> receivedMessagesFromOutput_B = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC1_B, 2);

        assertEquals(expectedResult_B, receivedMessagesFromOutput_B);

    }

}
