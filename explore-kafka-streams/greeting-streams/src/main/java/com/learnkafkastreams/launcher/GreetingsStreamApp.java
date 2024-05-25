package com.learnkafkastreams.launcher;

import com.learnkafkastreams.topology.GreetingTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class GreetingsStreamApp {

    private static Logger log = LoggerFactory.getLogger(GreetingsStreamApp.class.getName());

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        createTopics(properties, List.of(GreetingTopology.GREETINGS, GreetingTopology.GREETINGS_UPPERCASE));
        var greetingsTopology = GreetingTopology.builTopology();

        try (var kafkaStreams = new KafkaStreams(greetingsTopology, properties)) {
            kafkaStreams.start();
        }catch (Exception e) {
            log.error("Error in starting streams: {}", e.getMessage(), e);
        };


    }

    private static void createTopics(Properties config, List<String> greetings) {

        try (AdminClient admin = AdminClient.create(config)) {
            var partitions = 1;
            short replication = 1;

            var newTopics = greetings
                    .stream()
                    .map(topic -> {
                        return new NewTopic(topic, partitions, replication);
                    })
                    .collect(Collectors.toList());

            var createTopicResult = admin.createTopics(newTopics);
            try {
                createTopicResult
                        .all().get();
                log.info("topics are created successfully");
            } catch (Exception e) {
                log.error("Exception creating topics : {} ", e.getMessage(), e);
            }
        }
    }
}
