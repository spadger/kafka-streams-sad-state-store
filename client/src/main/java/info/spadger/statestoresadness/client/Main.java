package info.spadger.statestoresadness.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

public class Main {

    static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        String messageId = UUID.randomUUID().toString();
        log.info("Starting test with message-id: {}", messageId);

        ProducerRecord<String, String> first = new ProducerRecord<>("source-topic", messageId, messageId);
        producer.send(first);
        producer.flush();

        log.info("The streams topology should have crashed by design.");
        log.info("Specifically, before any record was published to the output topic");
        log.info("If the rocks-db state store is behaving correctly, the state store should be empty upon restart...");

        log.info("Restart the topology, and the message is reprocessed (good) but the state-store has persisted...");
    }
}
