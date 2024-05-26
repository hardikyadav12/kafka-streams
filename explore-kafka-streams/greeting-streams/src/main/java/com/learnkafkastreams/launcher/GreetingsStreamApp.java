package com.learnkafkastreams.launcher;

import com.learnkafkastreams.topology.GreetingTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class GreetingsStreamApp {


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        createTopics(properties, List.of(GreetingTopology.GREETINGS, GreetingTopology.GREETINGS_UPPERCASE, GreetingTopology.GREETINGS_SPANISH));
        var greetingsTopology = GreetingTopology.builTopology();
        var kafkaStreams = new KafkaStreams(greetingsTopology, properties);

        try {
            kafkaStreams.start();
        }catch(Exception e) {
            log.error("Error in starting KStream: {}", e.getMessage(), e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

    private static void createTopics(Properties config, List<String> greetings) throws ExecutionException, InterruptedException {

        try (AdminClient admin = AdminClient.create(config)) {
            var partitions = 1;
            short replication = 1;

            var newTopics = greetings
                    .stream()
                    .map(topic ->
                        new NewTopic(topic, partitions, replication)
                    ).toList();
            CreateTopicsResult createTopicResult;
                createTopicResult = admin.createTopics(newTopics);

            try {
                createTopicResult
                        .all().get();
                log.info("topics are created successfully");
            } catch (Exception e) {
                log.error("Exception creating topics : {} ", e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
    }
}
