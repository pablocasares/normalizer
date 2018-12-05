package io.wizzie.normalizer.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.StreamBuilder;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StreamFromMultipleInputTopicsIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT1_TOPIC = "input1";
    private static final String INPUT2_TOPIC = "input2";

    private static final String OUTPUT_TOPIC = "output";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic(INPUT1_TOPIC, 1, REPLICATION_FACTOR);
        CLUSTER.createTopic(INPUT2_TOPIC, 1, REPLICATION_FACTOR);

        // sinks
        CLUSTER.createTopic(OUTPUT_TOPIC, 1, REPLICATION_FACTOR);
    }

    @Test
    public void streamsShouldWorkWithKafka() throws InterruptedException, PlanBuilderException {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("stream-from-multiple-topics-integration.json").getFile());

        Properties streamsConfiguration = new Properties();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ObjectMapper mapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = mapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        Config config = new Config();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-1");

        StreamBuilder streamBuilder = new StreamBuilder(config, null);

        KafkaStreams streams = new KafkaStreams(streamBuilder.builder(model).build(), streamsConfiguration);

        streams.start();

        Map<String, Object> message1 = new HashMap<>();
        message1.put("stream", "stream1");

        Map<String, Object> message2 = new HashMap<>();
        message2.put("stream", "stream2");

        KeyValue<String, Map<String, Object>> kvStreams1 = new KeyValue<>("KEY_1", message1);
        KeyValue<String, Map<String, Object>> kvStreams2 = new KeyValue<>("KEY_2", message2);

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously(INPUT1_TOPIC, Arrays.asList(kvStreams1), producerConfig, MOCK_TIME);
            IntegrationTestUtils.produceKeyValuesSynchronously(INPUT2_TOPIC, Arrays.asList(kvStreams2), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            fail("Exception : " + e.getMessage());
        }

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-streams-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<KeyValue<String, Map>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, OUTPUT_TOPIC, 2);

        ArrayList<KeyValue<String, Map<String,Object>>> arrayListOfKvExpected = new ArrayList<>();
        arrayListOfKvExpected.add(new KeyValue<>("KEY_1", message1));
        arrayListOfKvExpected.add(new KeyValue<>("KEY_2", message2));

        assertEquals(arrayListOfKvExpected, result);

        streams.close();
        streamBuilder.close();

    }
}
