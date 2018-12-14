package io.wizzie.normalizer.builder;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.base.builder.config.ConfigProperties;
import io.wizzie.normalizer.base.utils.Constants;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.exceptions.TryToDoLoopException;
import io.wizzie.normalizer.funcs.*;
import io.wizzie.normalizer.model.FunctionModel;
import io.wizzie.normalizer.model.PlanModel;
import io.wizzie.normalizer.model.SinkModel;
import io.wizzie.normalizer.model.StreamModel;
import io.wizzie.normalizer.serializers.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class StreamBuilder {
    String appId;
    MetricsManager metricsManager;
    Config config;

    public StreamBuilder(Config config, MetricsManager metricsManager) {
        this.appId = config.get(APPLICATION_ID_CONFIG);
        this.config = config;
        this.metricsManager = metricsManager;
    }

    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    private Map<String, KStream<String, Map<String, Object>>> kStreams = new HashMap<>();
    private Map<String, Map<String, Function>> streamFunctions = new HashMap<>();
    private Map<String, Map<String, FilterFunc>> streamFilters = new HashMap<>();
    private Map<String, Integer> streamIterationCreation = new HashMap<>();
    private Set<String> streamEdges = new HashSet<>();
    private Set<String> usedStores = new HashSet<>();
    private Set<String> addedFuncsToStreams = new HashSet<>();
    private Set<String> addedSinksToStreams = new HashSet<>();
    private Boolean addedNewStream = true;

    public StreamsBuilder builder(PlanModel model) throws PlanBuilderException {
        model.validate(config);

        clean();

        StreamsBuilder builder = new StreamsBuilder();
        createInputStreams(builder, model);

        for (int iteration = 0; addedNewStream; iteration++) {
            log.info("*** Iteration [{}]", iteration);
            addedNewStream = false;
            addFuncsToStreams(builder, model, iteration);
            addSinksToStreams(model, iteration);
        }

        return builder;
    }

    public KStream<String, Map<String, Object>> getKStream(String streamName) {
        return kStreams.get(streamName);
    }

    public Map<String, Function> getFunctions(String streamName) {
        return streamFunctions.get(streamName);
    }

    public Map<String, FilterFunc> getFilters(String streamName) {
        return streamFilters.get(streamName);
    }

    public Set<String> usedStores() {
        return usedStores;
    }

    public void close() {
        streamFunctions.forEach((stream, functions) -> functions.forEach((name, function) -> function.stop()));
        clean();
    }

    private void createInputStreams(StreamsBuilder builder, PlanModel model) {
        for (Map.Entry<String, List<String>> inputs : model.getInputs().entrySet()) {
            String topic = inputs.getKey();

            if (config.getOrDefault(ConfigProperties.MULTI_ID, false)) {
                topic = String.format("%s_%s", appId, topic);
            }

            KStream<String, Map<String, Object>> kstream = builder.stream(topic);

            for (String stream : inputs.getValue()) {
                if (!kStreams.containsKey(stream)) {
                    log.info("Creating stream [{}]", stream);
                    kStreams.put(stream, kstream.filter((key, value) -> key != null || value != null));
                    streamIterationCreation.put(stream, 0);
                } else {
                    kStreams.put(stream, kStreams.get(stream).merge(kstream));
                }

                streamEdges.add(String.format("%s->%s", topic, stream));
            }
        }
    }

    private void addFuncsToStreams(StreamsBuilder builder, PlanModel model, Integer iteration) {
        for (Map.Entry<String, StreamModel> stream : model.getStreams().entrySet()) {
            if (!addedFuncsToStreams.contains(stream.getKey()) && kStreams.containsKey(stream.getKey())) {
                List<FunctionModel> funcModels = stream.getValue().getFuncs();
                if (funcModels != null) {
                    for (FunctionModel funcModel : funcModels) {
                        KStream<String, Map<String, Object>> kStream = kStreams.get(stream.getKey());
                        String name = funcModel.getName();
                        String className = funcModel.getClassName();
                        Map<String, Object> properties = funcModel.getProperties();
                        List<String> stores = funcModel.getStores();

                        if (stores != null) {
                            properties.put(Constants.__STORES, stores);
                            properties.put(Constants.__APP_ID, appId);

                            stores = stores.stream()
                                    .map(store -> String.format("%s_%s", appId, store))
                                    .collect(Collectors.toList());

                            stores.forEach(store -> {
                                if (!usedStores.contains(store)) {
                                    StoreBuilder storeSupplier = Stores
                                            .keyValueStoreBuilder(Stores.persistentKeyValueStore(store),
                                                    Serdes.String(),
                                                    new JsonSerde()
                                            );

                                    builder.addStateStore(storeSupplier);
                                    usedStores.add(store);
                                }
                            });
                        }

                        try {
                            log.info("Add function [{}] to stream [{}]", name, stream.getKey());
                            Function func = makeFunction(className, properties);
                            if (func instanceof MapperFunction) {
                                kStream = kStream.map((MapperFunction) func);
                            } else if (func instanceof FlatMapperFunction) {
                                kStream = kStream.flatMap((FlatMapperFunction) func);
                            } else if (func instanceof MapperStoreFunction) {
                                kStream = kStream.transform(() ->
                                        (MapperStoreFunction) func, stores.toArray(new String[stores.size()])
                                );
                            } else if (func instanceof FilterFunc) {
                                FilterFunc filterFunc = (FilterFunc) func;
                                kStream = kStream.filter(filterFunc);
                            }

                            Map<String, Function> functions = streamFunctions.get(stream.getKey());
                            if (functions == null) functions = new HashMap<>();
                            functions.put(name, func);

                            streamFunctions.put(stream.getKey(), functions);
                            kStreams.put(stream.getKey(), kStream);

                            if (!streamIterationCreation.containsKey(stream.getKey())) {
                                streamIterationCreation.put(stream.getKey(), iteration);
                            }
                        } catch (ClassNotFoundException e) {
                            log.error("Couldn't find the class associated with the function {}", className);
                        } catch (InstantiationException | IllegalAccessException e) {
                            log.error("Couldn't create the instance associated with the function " + className, e);
                        }
                    }
                }

                addedFuncsToStreams.add(stream.getKey());
            } else {
                if (!kStreams.containsKey(stream.getKey())) {
                    log.debug("Stream {} is to later iteration.", stream.getKey());
                }
            }
        }
    }

    private void addSinksToStreams(PlanModel model, Integer iteration) throws TryToDoLoopException {
        List<String> generatedStreams = new ArrayList<>();
        for (Map.Entry<String, StreamModel> stream : model.getStreams().entrySet()) {
            Map<String, FilterFunc> filters = new HashMap<>();
            if (!addedSinksToStreams.contains(stream.getKey()) && !generatedStreams.contains(stream.getKey())) {
                List<SinkModel> sinks = stream.getValue().getSinks();
                for (SinkModel sink : sinks) {
                    KStream<String, Map<String, Object>> kStream = kStreams.get(stream.getKey());
                    if (kStream != null) {
                        kStream = kStream.filter((key, value) -> key != null || value != null);
                        log.info("Send to {} [{}]", sink.getType(), sink.getTopic());

                        if (!sink.getPartitionBy().equals(SinkModel.PARTITION_BY_KEY)) {
                            kStream = kStream.selectKey((key, value) -> {
                                Object newKey = null;
                                if (value != null) {
                                    newKey = value.get(sink.getPartitionBy());
                                }
                                if (newKey != null) return newKey.toString();
                                else {
                                    log.trace("Partition key {} isn't on message {}",
                                            sink.getPartitionBy(), value);
                                    return key;
                                }
                            });
                        }

                        FunctionModel filterModel = sink.getFilter();
                        if (filterModel != null) {
                            String className = filterModel.getClassName();
                            try {
                                FilterFunc filter = (FilterFunc) makeFunction(className, filterModel.getProperties());
                                kStream = kStream.filter(filter);
                                filters.put(String.format("%s-%s", sink.getType(), sink.getTopic()), filter);
                            } catch (ClassNotFoundException e) {
                                log.error("Couldn't find the class associated with the function {}", className);
                            } catch (InstantiationException | IllegalAccessException e) {
                                log.error("Couldn't create the instance associated with the function " + className, e);
                            }
                        }

                        if (sink.getType().equals(SinkModel.KAFKA_TYPE)) {
                            String topic = sink.getTopic();

                            if (config.getOrDefault(ConfigProperties.MULTI_ID, false)) {
                                topic = String.format("%s_%s", appId, topic);
                            }

                            streamIterationCreation.put(stream.getKey(), iteration);
                            streamEdges.add(String.format("%s->%s", stream.getKey(), topic));

                            kStream.to(topic);
                        } else if (sink.getType().equals(SinkModel.STREAM_TYPE)) {
                            String newStreamName = sink.getTopic();

                            if (newStreamName.equals(stream.getKey())) {
                                throw new TryToDoLoopException(
                                        "Loop from [" + stream.getKey() + "] to [" + newStreamName + "]"
                                );
                            }

                            if (!kStreams.containsKey(newStreamName)) {
                                addedNewStream = true;
                                KStream<String, Map<String, Object>> newBranch = kStream.branch((key, value) -> true)[0];
                                kStreams.put(newStreamName, newBranch);

                                if (!streamIterationCreation.containsKey(newStreamName)) {
                                    streamIterationCreation.put(newStreamName, iteration);
                                }

                                streamEdges.add(String.format("%s->%s", stream.getKey(), sink.getTopic()));
                                log.info("Creating stream [{}]", sink.getTopic());
                                generatedStreams.add(sink.getTopic());
                            } else {
                                int sinkStreamIteration = streamIterationCreation.get(newStreamName);

                                if (
                                        (((sinkStreamIteration + 1) == iteration) || (sinkStreamIteration == iteration)) && !streamEdges.contains(String.format("%s->%s", newStreamName, stream.getKey()))) {
                                    KStream<String, Map<String, Object>> mergeKStream = kStreams.get(newStreamName).merge(kStream);
                                    kStreams.put(newStreamName, mergeKStream);
                                    streamEdges.add(String.format("%s->%s", stream.getKey(), newStreamName));
                                } else {
                                    throw new TryToDoLoopException(
                                            "Loop from [" + stream.getKey() + "] to [" + newStreamName + "]"
                                    );
                                }

                            }
                        }
                        streamFilters.put(stream.getKey(), filters);
                        addedSinksToStreams.add(stream.getKey());
                    }
                }
            }
        }
        generatedStreams.clear();
    }

    private void clean() {
        addedSinksToStreams.clear();
        streamFunctions.clear();
        addedFuncsToStreams.clear();
        usedStores.clear();
        kStreams.clear();
        streamEdges.clear();
        streamIterationCreation.clear();
        addedNewStream = true;
    }

    private Function makeFunction(String className, Map<String, Object> properties)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class funcClass = Class.forName(className);
        Function func = (Function) funcClass.newInstance();
        func.init(properties, metricsManager);
        return func;
    }
}
