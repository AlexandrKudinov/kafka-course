package ru.practice.kafka.practical3.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.practice.kafka.practical3.service.MessageStreamProcessor;

import java.util.Properties;

@Configuration
public class KafkaStreamsConfig {

    @Bean
    public Properties streamsProperties(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.streams.application-id}") String applicationId,
            @Value("${kafka.streams.state-dir}") String stateDir,
            @Value("${kafka.streams.auto-offset-reset}") String autoOffsetReset,
            @Value("${kafka.streams.num-stream-threads}") int numStreamThreads,
            @Value("${kafka.streams.cache-max-bytes-buffering}") long cacheMaxBytesBuffering,
            @Value("${kafka.streams.commit-interval-ms}") long commitIntervalMs) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheMaxBytesBuffering);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put("auto.offset.reset", autoOffsetReset);
        return properties;
    }

    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean
    public KStream<String, String> messagesStream(
            StreamsBuilder builder,
            @Value("${kafka.topics.messages}") String messagesTopic) {
        return builder.stream(messagesTopic, Consumed.with(Serdes.String(), Serdes.String()));
    }

    @Bean
    public KTable<String, String> blockedUsersTable(
            StreamsBuilder builder,
            @Value("${kafka.topics.blocked-users}") String blockedUsersTopic) {
        return builder.table(
                blockedUsersTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("blocked-users-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );
    }

    @Bean
    public GlobalKTable<String, String> bannedWordsTable(
            StreamsBuilder builder,
            @Value("${kafka.topics.banned-words}") String bannedWordsTopic) {
        return builder.globalTable(
                bannedWordsTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );
    }

    @Bean
    public KStream<String, String> filteredMessagesStream(
            KStream<String, String> messagesStream,
            KTable<String, String> blockedUsersTable,
            GlobalKTable<String, String> bannedWordsTable,
            MessageStreamProcessor processor,
            @Value("${kafka.topics.filtered-messages}") String filteredMessagesTopic) {

        KStream<String, String> afterBlocking = messagesStream
                .leftJoin(blockedUsersTable, processor::filterByBlockedUser)
                .filter((key, value) -> value != null && !"BLOCKED".equals(value));

        KStream<String, String> filtered = afterBlocking
                .leftJoin(
                        bannedWordsTable,
                        (key, value) -> "global",
                        processor::applyCensorship
                )
                .filter((key, value) -> value != null);

        filtered.to(filteredMessagesTopic, Produced.with(Serdes.String(), Serdes.String()));
        return filtered;
    }

    @Bean
    public KafkaStreams kafkaStreams(
            StreamsBuilder builder,
            Properties streamsProperties,
            KStream<String, String> filteredMessagesStream) {
        return new KafkaStreams(builder.build(), streamsProperties);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
