package info.spadger.statestoresadness.topology;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TopicCreator {

    private final Properties config;

    public TopicCreator(final String broker) {
        config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    }

    void create(final String... topicNames) {

        try (final AdminClient adminClient = KafkaAdminClient.create(config)) {

            for(String topicName : topicNames) {
                try {
                    final NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

                    final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));

                    // Since the call is Async, Lets wait for it to complete.
                    createTopicsResult.values().get(topicName).get();
                } catch (InterruptedException | ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                    // TopicExistsException - Swallow this exception, just means the topic already exists.
                }
            }
        }
    }
}
