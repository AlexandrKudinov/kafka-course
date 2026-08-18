package ru.practice.kafka.practical3;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import ru.practice.kafka.practical3.service.MessageStreamProcessor;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageFilteringTopologyTest {

    @Test
    void shouldFilterBlockedUserAndCensorAllowedMessage() {
        StreamsBuilder builder = new StreamsBuilder();
        MessageStreamProcessor processor = new MessageStreamProcessor(new ObjectMapper());

        KStream<String, String> messages = builder.stream(
                "messages", Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, String> blockedUsers = builder.table(
                "blocked_users",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("blocked-users-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        GlobalKTable<String, String> bannedWords = builder.globalTable(
                "banned_words", Consumed.with(Serdes.String(), Serdes.String()));

        messages
                .leftJoin(blockedUsers, processor::filterByBlockedUser)
                .filter((key, value) -> value != null && !"BLOCKED".equals(value))
                .leftJoin(bannedWords, (key, value) -> "global", processor::applyCensorship)
                .filter((key, value) -> value != null)
                .to("filtered_messages", Produced.with(Serdes.String(), Serdes.String()));

        Properties properties = new Properties();
        properties.put("application.id", "practical-3-test");
        properties.put("bootstrap.servers", "dummy:9092");
        properties.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );

        properties.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties)) {
            TestInputTopic<String, String> blockedInput = driver.createInputTopic(
                    "blocked_users", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> bannedInput = driver.createInputTopic(
                    "banned_words", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> messagesInput = driver.createInputTopic(
                    "messages", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    "filtered_messages", Serdes.String().deserializer(), Serdes.String().deserializer());

            blockedInput.pipeInput("user-1", "{\"userIds\":[\"user-2\"]}");
            bannedInput.pipeInput("global", "{\"words\":[\"bad\"]}");

            messagesInput.pipeInput("user-1", "{\"id\":\"blocked\",\"senderId\":\"user-2\",\"recipientId\":\"user-1\",\"text\":\"hello\"}");
            messagesInput.pipeInput("user-1", "{\"id\":\"allowed\",\"senderId\":\"user-3\",\"recipientId\":\"user-1\",\"text\":\"hello bad\"}");

            assertEquals(1, output.getQueueSize());
            assertTrue(output.readValue().contains("hello ***"));
        }
    }
}
