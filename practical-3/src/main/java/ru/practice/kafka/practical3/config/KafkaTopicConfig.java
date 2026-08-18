package ru.practice.kafka.practical3.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic messagesTopic(
            @Value("${kafka.topics.messages}") String name,
            @Value("${kafka.topics-config.partitions}") int partitions,
            @Value("${kafka.topics-config.replication-factor}") short replicationFactor) {
        return new NewTopic(name, partitions, replicationFactor);
    }

    @Bean
    public NewTopic filteredMessagesTopic(
            @Value("${kafka.topics.filtered-messages}") String name,
            @Value("${kafka.topics-config.partitions}") int partitions,
            @Value("${kafka.topics-config.replication-factor}") short replicationFactor) {
        return new NewTopic(name, partitions, replicationFactor);
    }

    @Bean
    public NewTopic blockedUsersTopic(
            @Value("${kafka.topics.blocked-users}") String name,
            @Value("${kafka.topics-config.partitions}") int partitions,
            @Value("${kafka.topics-config.replication-factor}") short replicationFactor) {
        return new NewTopic(name, partitions, replicationFactor);
    }

    @Bean
    public NewTopic bannedWordsTopic(
            @Value("${kafka.topics.banned-words}") String name,
            @Value("${kafka.topics-config.partitions}") int partitions,
            @Value("${kafka.topics-config.replication-factor}") short replicationFactor) {
        return new NewTopic(name, partitions, replicationFactor);
    }
}
