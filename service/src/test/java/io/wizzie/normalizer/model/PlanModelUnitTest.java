package io.wizzie.normalizer.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.base.builder.config.ConfigProperties;
import io.wizzie.normalizer.exceptions.MaxOutputKafkaTopics;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class PlanModelUnitTest {

    @Test
    public void throwExceptionInDuplicatedStreamsTest() {
        Map<String, List<String>> inputs = new HashMap<>();
        inputs.put("topic1", Arrays.asList("stream1", "stream1"));

        // StreamModel mock
        StreamModel streamModelMockObject = mock(StreamModel.class);

        Map<String, StreamModel> streams = new HashMap<>();
        streams.put("stream1", streamModelMockObject);

        PlanModel planModelObject = new PlanModel(inputs, streams);

        assertNotNull(planModelObject.getInputs());
        assertEquals(planModelObject.getInputs(), inputs);

        assertNotNull(planModelObject.getStreams());
        assertEquals(planModelObject.getStreams(), streams);

        try {
            Config config = new Config();
            planModelObject.validate(config);
            assertNotNull(planModelObject.getDefinedStreams());
        } catch (PlanBuilderException e) {
            assertEquals(e.getMessage(), "Stream[stream1]: Duplicated");
        }
    }

    @Test
    public void throwExceptionInNotDefinedInputStreamTest() {
        Map<String, List<String>> inputs = new HashMap<>();
        inputs.put("topic1", Arrays.asList("stream1", "stream2"));

        //StreamModel mock
        StreamModel streamModelObjectMock = mock(StreamModel.class);

        Map<String, StreamModel> streams = new HashMap<>();
        streams.put("notValidStream", streamModelObjectMock);

        PlanModel planModelObject = new PlanModel(inputs, streams);

        assertNotNull(planModelObject.getInputs());
        assertEquals(planModelObject.getInputs(), inputs);

        assertNotNull(planModelObject.getStreams());
        assertEquals(planModelObject.getStreams(), streams);

        try {
            Config config = new Config();
            planModelObject.validate(config);
            assertNotNull(planModelObject.getDefinedStreams());
        } catch (PlanBuilderException e) {
            assertEquals("Stream[notValidStream]: Not defined on inputs. Available definedStreams [stream1, stream2]", e.getMessage());
        }
    }

    @Test(expected = MaxOutputKafkaTopics.class)
    public void throwExceptionMaxOutputKafkaTopicsSingleStream() throws PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("multiple-kafka-sinks-1stream.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        Config config = new Config();
        config.put(ConfigProperties.MAX_KAFKA_OUTPUT_TOPICS, 1);
        model.validate(config);
    }

    @Test(expected = MaxOutputKafkaTopics.class)
    public void throwExceptionMaxOutputKafkaTopicsMultipleStreams() throws PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("multiple-kafka-sinks-multiple-streams.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        Config config = new Config();
        config.put(ConfigProperties.MAX_KAFKA_OUTPUT_TOPICS, 1);
        model.validate(config);
    }

    @Test(expected = PlanBuilderException.class)
    public void throwExceptionFilterNotNull() throws PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("filter-not-null.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        Config config = new Config();
        config.put(ConfigProperties.MAX_KAFKA_OUTPUT_TOPICS, 2);
        model.validate(config);
    }

    @Test(expected = PlanBuilderException.class)
    public void throwExceptionFilterNotEmpty() throws PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("filter-not-null.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        Config config = new Config();
        config.put(ConfigProperties.MAX_KAFKA_OUTPUT_TOPICS, 2);
        model.validate(config);
    }
}
