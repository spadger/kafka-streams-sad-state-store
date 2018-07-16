package info.spadger.statestoresadness.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

public class Main {

    static final Logger log = LoggerFactory.getLogger(Main.class);

    static final String BROKER = "PLAINTEXT://localhost:9092";
    static final String STATE_STORE_NAME = "some-state-store";
    static final String SOURCE_TOPIC_NAME = "source-topic";
    static final String SINK_TOPIC_NAME = "output-sink";

    public static void main(String[] args) {

        TopicCreator creator = new TopicCreator(BROKER);
        creator.create(SOURCE_TOPIC_NAME);
        creator.create(SINK_TOPIC_NAME);
        doit();
    }


    private static void doit() {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "statestoresadness");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/statestoresadness/");

        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(STATE_STORE_NAME),
            Serdes.String(),
            Serdes.String());

        Topology topology = new StreamsBuilder()
            .build()
            .addSource("source", SOURCE_TOPIC_NAME)

            .addProcessor("processor", () -> new AbstractProcessor<String, String>() {

                KeyValueStore<String, String> stateStore;

                @Override
                public void process(final String key, final String value) {

                    String storedKey = stateStore.get(key);

                    log.info("===BEGIN ALL STORED VALUES===");
                    stateStore.all().forEachRemaining(x -> log.info("{} : {}", x.key, x.value));
                    log.info("===END ALL STORED VALUES===");

                    if(storedKey != null) {
                        log.error("{}:{} was found in the state store!!!", key, value);
                        System.exit(0);
                    }

                    log.info("Message`{}` was not a duplicate, lets register is in the state store", key);
                    stateStore.put(key, value);
                    stateStore.flush();
                    log.info("Now to throw an error before forwarding the message...");

                    if (true) {
                        throw new RuntimeException("Some random error...");
                    }

                    ///This never happens, of course....
                    this.context().forward(key, value);
                }

                @Override
                public void init(ProcessorContext context) {
                    super.init(context);
                    stateStore = (KeyValueStore<String, String>)context().getStateStore(STATE_STORE_NAME);
                }
            }, "source")

            .addSink("sink", SINK_TOPIC_NAME, "processor")
            .addStateStore(storeBuilder, "processor");


        KafkaStreams streams = new KafkaStreams(topology, config);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
