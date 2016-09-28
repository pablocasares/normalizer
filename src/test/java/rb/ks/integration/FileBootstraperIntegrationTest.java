package rb.ks.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import rb.ks.builder.Builder;
import rb.ks.builder.config.Config;
import rb.ks.serializers.JsonDeserializer;
import rb.ks.serializers.JsonSerde;
import rb.ks.serializers.JsonSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;
import static rb.ks.builder.config.Config.ConfigProperties.BOOTSTRAPER_CLASSNAME;

public class FileBootstraperIntegrationTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_TOPIC = "input1";

    private static final String OUTPUT_TOPIC1 = "output1";
    private static final String OUTPUT_TOPIC2 = "output2";

    private static final String BOOTSTRAP_TOPIC = "__normalizer_bootstrap";

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(OUTPUT_TOPIC1, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(OUTPUT_TOPIC2, 2, REPLICATION_FACTOR);

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
    public void fileBootstraperShouldWork() throws Exception {
        Map<String, Object> streamsConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Config configuration = new Config(streamsConfiguration);
        configuration.put("file.bootstraper.path", Thread.currentThread().getContextClassLoader().getResource("kafka-bootstraper-integration-test-1.json").getFile());
        configuration.put(BOOTSTRAPER_CLASSNAME, "rb.ks.builder.bootstrap.FileBootstraper");

        Builder builder = new Builder(configuration);

        Map<String, Object> b1 = new HashMap<>();
        b1.put("C", 6000L);

        Map<String, Object> a1 = new HashMap<>();
        a1.put("B", b1);

        Map<String, Object> message1 = new HashMap<>();
        message1.put("A", a1);

        message1.put("timestamp", 1122334455L);

        KeyValue<String, Map<String, Object>> kvStream1 = new KeyValue<>("KEY_A", message1);

        Map<String, Object> b2 = new HashMap<>();
        b2.put("C", 9000L);

        Map<String, Object> a2 = new HashMap<>();
        a2.put("B", b2);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("A", a2);

        message2.put("timestamp", 1122334655L);

        KeyValue<String, Map<String, Object>> kvStream2 = new KeyValue<>("KEY_A", message2);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Arrays.asList(kvStream1, kvStream2), producerConfig);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("X", 3000);
        expectedData.put("timestamp", 1122334655);
        expectedData.put("last_timestamp", 1122334455);

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>("KEY_A", expectedData);

        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC1, 1);

        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @AfterClass
    public static void stopKafkaCluster() {
        CLUSTER.stop();
    }

    public static String getFileContent(File file) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        StringBuilder stringBuffer = new StringBuilder();

        String line;

        while ((line = bufferedReader.readLine()) != null) {

            stringBuffer.append(line).append("\n");
        }

        return stringBuffer.toString();
    }

}
