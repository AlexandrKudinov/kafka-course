package ru.practice.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.producer.acks}") String acks,
            @Value("${spring.kafka.producer.retries}") int retries,
            @Value("${spring.kafka.producer.properties.max.in.flight.requests.per.connection}") int maxInFlight,
            @Value("${spring.kafka.producer.properties.linger.ms}") int lingerMs) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        props.put("acks", acks);
        props.put("retries", retries);
        props.put("max.in.flight.requests.per.connection", maxInFlight);
        props.put("linger.ms", lingerMs);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, String> singleConsumerFactory(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.consumers.single.enable-auto-commit}") boolean autoCommit,
            @Value("${kafka.consumers.single.max-poll-records}") int maxPollRecords,
            @Value("${spring.kafka.consumer.properties.fetch.min.bytes}") int fetchMinBytes,
            @Value("${spring.kafka.consumer.properties.fetch.max.wait.ms}") int fetchMaxWaitMs,
            @Value("${spring.kafka.consumer.properties.max.poll.interval.ms}") int maxPollIntervalMs) {
        return new DefaultKafkaConsumerFactory<>(consumerProperties(
                bootstrapServers, autoCommit, maxPollRecords,
                fetchMinBytes, fetchMaxWaitMs, maxPollIntervalMs));
    }

    @Bean
    public ConsumerFactory<String, String> batchConsumerFactory(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.consumers.batch.enable-auto-commit}") boolean autoCommit,
            @Value("${kafka.consumers.batch.max-poll-records}") int maxPollRecords,
            @Value("${spring.kafka.consumer.properties.fetch.min.bytes}") int fetchMinBytes,
            @Value("${spring.kafka.consumer.properties.fetch.max.wait.ms}") int fetchMaxWaitMs,
            @Value("${spring.kafka.consumer.properties.max.poll.interval.ms}") int maxPollIntervalMs) {
        return new DefaultKafkaConsumerFactory<>(consumerProperties(
                bootstrapServers, autoCommit, maxPollRecords,
                fetchMinBytes, fetchMaxWaitMs, maxPollIntervalMs));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> singleKafkaListenerContainerFactory(
            ConsumerFactory<String, String> singleConsumerFactory,
            @Value("${kafka.consumers.single.poll-timeout-ms}") long pollTimeoutMs) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(singleConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setPollTimeout(pollTimeoutMs);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, String> batchConsumerFactory,
            @Value("${kafka.consumers.batch.poll-timeout-ms}") long pollTimeoutMs) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(batchConsumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.getContainerProperties().setPollTimeout(pollTimeoutMs);
        return factory;
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    private Map<String, Object> consumerProperties(
            String bootstrapServers,
            boolean autoCommit,
            int maxPollRecords,
            int fetchMinBytes,
            int fetchMaxWaitMs,
            int maxPollIntervalMs) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        return props;
    }
}
