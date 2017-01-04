package io.wizzie.ks.normalizer.builder;

import io.wizzie.ks.normalizer.builder.config.Config;
import io.wizzie.ks.normalizer.exceptions.PlanBuilderException;
import io.wizzie.ks.normalizer.exceptions.TryToDoLoopException;
import io.wizzie.ks.normalizer.funcs.*;
import io.wizzie.ks.normalizer.metrics.MetricsManager;
import io.wizzie.ks.normalizer.model.FunctionModel;
import io.wizzie.ks.normalizer.model.PlanModel;
import io.wizzie.ks.normalizer.model.SinkModel;
import io.wizzie.ks.normalizer.model.StreamModel;
import io.wizzie.ks.normalizer.serializers.JsonSerde;
import kafka.admin.TopicCommand;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.wizzie.ks.normalizer.utils.Constants.__APP_ID;
import static io.wizzie.ks.normalizer.utils.Constants.__STORES;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class StreamBuilder {
    String appId;
    MetricsManager metricsManager;
    Config config;
    ZkUtils zkUtils;
    ZkClient zkClient;

    final String ZK_CONNECT;
    final String PARTITIONS = "4";
    final String REPLICATION_FACTOR = "1";

    final int ZK_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);

    public StreamBuilder(Config config, MetricsManager metricsManager) {
        ZK_CONNECT = config.get("zookeeper.connect");

        this.appId = config.get(APPLICATION_ID_CONFIG);
        this.config = config;
        this.metricsManager = metricsManager;

        if(ZK_CONNECT != null) {
            zkClient = createZkClient();
            zkUtils = new ZkUtils(zkClient, new ZkConnection(ZK_CONNECT),false);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    private Map<String, KStream<String, Map<String, Object>>> kStreams = new HashMap<>();
    private Map<String, Map<String, Function>> streamFunctions = new HashMap<>();
    private Map<String, Map<String, FilterFunc>> streamFilters = new HashMap<>();
    private Set<String> usedStores = new HashSet<>();
    private Set<String> addedFuncsToStreams = new HashSet<>();
    private Set<String> addedSinksToStreams = new HashSet<>();
    private Boolean addedNewStream = true;

    public KStreamBuilder builder(PlanModel model) throws PlanBuilderException {
        model.validate();

        clean();

        KStreamBuilder builder = new KStreamBuilder();
        createInputStreams(builder, model);

        for (int iteration = 0; addedNewStream; iteration++) {
            log.info("*** Iteration [{}]", iteration);
            addedNewStream = false;
            addFuncsToStreams(builder, model);
            addSinksToStreams(model);
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
        streamFunctions.forEach((stream, functions) -> functions.forEach((name, fucntion) -> fucntion.stop()));
        clean();
    }

    private ZkClient createZkClient(){
        return new ZkClient(ZK_CONNECT, ZK_TIMEOUT, ZK_TIMEOUT, ZKStringSerializer$.MODULE$);
    }

    private void createTopicIfNotExists(String topic) {
        if(zkClient != null && zkUtils != null) {
            String[] topicArgs = {
                    "--zookeeper", ZK_CONNECT,
                    "--partitions", PARTITIONS,
                    "--replication-factor", REPLICATION_FACTOR,
                    "--create",
                    "--topic", topic,
                    "--if-not-exists"
            };

            TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(topicArgs);
            options.checkArgs();

            TopicCommand.createTopic(zkUtils, options);
        }
    }

    private void createInputStreams(KStreamBuilder builder, PlanModel model) {
        for (Map.Entry<String, List<String>> inputs : model.getInputs().entrySet()) {
            String topic = inputs.getKey();

            if (config.getOrDefault(Config.ConfigProperties.MULTI_ID, false)) {
                topic = String.format("%s_%s", appId, topic);
            }

            createTopicIfNotExists(topic);

            KStream<String, Map<String, Object>> kstream = builder.stream(topic);
            for (String stream : inputs.getValue()) {
                log.info("Creating stream [{}]", stream);
                kStreams.put(stream, kstream);
            }
        }
    }

    private void addFuncsToStreams(KStreamBuilder builder, PlanModel model) {
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            if (!addedFuncsToStreams.contains(streams.getKey()) && kStreams.containsKey(streams.getKey())) {
                List<FunctionModel> funcModels = streams.getValue().getFuncs();
                if (funcModels != null) {
                    for (FunctionModel funcModel : funcModels) {
                        KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());
                        String name = funcModel.getName();
                        String className = funcModel.getClassName();
                        Map<String, Object> properties = funcModel.getProperties();
                        List<String> stores = funcModel.getStores();

                        if (stores != null) {
                            properties.put(__STORES, stores);
                            properties.put(__APP_ID, appId);

                            stores = stores.stream()
                                    .map(store -> String.format("%s_%s", appId, store))
                                    .collect(Collectors.toList());

                            stores.forEach(store -> {
                                if (!usedStores.contains(store)) {
                                    StateStoreSupplier storeSupplier = Stores.create(store)
                                            .withKeys(Serdes.String())
                                            .withValues(new JsonSerde())
                                            .persistent()
                                            .build();

                                    builder.addStateStore(storeSupplier);
                                    usedStores.add(store);
                                }
                            });
                        }

                        try {
                            log.info("Add function [{}] to stream [{}]", name, streams.getKey());
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
                                if (filterFunc.match()) {
                                    kStream = kStream.filter(filterFunc);
                                } else {
                                    kStream = kStream.filterNot(filterFunc);
                                }

                            }

                            Map<String, Function> functions = streamFunctions.get(streams.getKey());
                            if (functions == null) functions = new HashMap<>();
                            functions.put(name, func);

                            streamFunctions.put(streams.getKey(), functions);
                            kStreams.put(streams.getKey(), kStream);
                        } catch (ClassNotFoundException e) {
                            log.error("Couldn't find the class associated with the function {}", className);
                        } catch (InstantiationException | IllegalAccessException e) {
                            log.error("Couldn't create the instance associated with the function " + className, e);
                        }
                    }
                }

                addedFuncsToStreams.add(streams.getKey());
            } else {
                if (!kStreams.containsKey(streams.getKey())) {
                    log.debug("Stream {} is to later iteration.", streams.getKey());
                }
            }
        }
    }

    private void addSinksToStreams(PlanModel model) throws TryToDoLoopException {
        List<String> generatedStreams = new ArrayList<>();
        for (Map.Entry<String, StreamModel> streams : model.getStreams().entrySet()) {
            Map<String, FilterFunc> filters = new HashMap<>();
            if (!addedSinksToStreams.contains(streams.getKey()) && !generatedStreams.contains(streams.getKey())) {
                List<SinkModel> sinks = streams.getValue().getSinks();
                for (SinkModel sink : sinks) {
                    KStream<String, Map<String, Object>> kStream = kStreams.get(streams.getKey());
                    if (kStream != null) {
                        log.info("Send to {} [{}]", sink.getType(), sink.getTopic());

                        if (!sink.getPartitionBy().equals(SinkModel.PARTITION_BY_KEY)) {
                            kStream = kStream.map(
                                    (key, value) -> {
                                        Object newKey = value.get(sink.getPartitionBy());
                                        if (newKey != null)
                                            return new KeyValue<>(newKey.toString(), value);
                                        else {
                                            log.warn("Partition key {} isn't on message {}",
                                                    sink.getPartitionBy(), value);
                                            return new KeyValue<>(key, value);
                                        }
                                    }
                            );
                        }

                        FunctionModel filterModel = sink.getFilter();
                        if (filterModel != null) {
                            String className = filterModel.getClassName();
                            try {
                                FilterFunc filter = (FilterFunc) makeFunction(className, filterModel.getProperties());
                                if (filter.match()) {
                                    kStream = kStream.filter(filter);
                                } else {
                                    kStream = kStream.filterNot(filter);
                                }
                                filters.put(String.format("%s-%s", sink.getType(), sink.getTopic()), filter);
                            } catch (ClassNotFoundException e) {
                                log.error("Couldn't find the class associated with the function {}", className);
                            } catch (InstantiationException | IllegalAccessException e) {
                                log.error("Couldn't create the instance associated with the function " + className, e);
                            }
                        }

                        if (sink.getType().equals(SinkModel.KAFKA_TYPE)) {
                            String topic = sink.getTopic();

                            if (config.getOrDefault(Config.ConfigProperties.MULTI_ID, false)) {
                                topic = String.format("%s_%s", appId, topic);
                            }

                            createTopicIfNotExists(topic);

                            kStream.to(
                                    (key, value, numPartitions) ->
                                            Utils.abs(Utils.murmur2(key.getBytes())) % numPartitions, topic

                            );
                        } else if (sink.getType().equals(SinkModel.STREAM_TYPE)) {
                            String newStreamName = sink.getTopic();
                            if (!kStreams.containsKey(newStreamName)) {
                                addedNewStream = true;
                                KStream<String, Map<String, Object>> newBranch;

                                if (sink.getPartitionBy().equals(SinkModel.PARTITION_BY_KEY)) {
                                    newBranch = kStream.branch((key, value) -> true)[0];
                                } else {
                                    newBranch = kStream.through(
                                            (key, value, numPartitions) -> {
                                                if (key == null) key = "NULL_KEY";
                                                return Utils.abs(Utils.murmur2(key.getBytes())) % numPartitions;
                                            },
                                            String.format("__%s_normalizer_%s_to_%s", appId, streams.getKey(), newStreamName)
                                    );
                                }

                                kStreams.put(newStreamName, newBranch);
                                log.info("Creating stream [{}]", sink.getTopic());
                                generatedStreams.add(sink.getTopic());
                            } else {
                                throw new TryToDoLoopException(
                                        "Loop from [" + streams.getKey() + "] to [" + newStreamName + "]"
                                );
                            }
                        }
                        streamFilters.put(streams.getKey(), filters);
                        addedSinksToStreams.add(streams.getKey());
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
