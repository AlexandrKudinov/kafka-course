package ru.practice.kafka.practical7;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Component
public class KafkaMessageConsumer {

    private final String topic;
    private final String bootstrapServers;
    private final String groupId;

    public KafkaMessageConsumer(
            @Value("${kafka.topic}") String topic,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.consumer.group-id}") String groupId) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    public void consumeTestMessages() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000;

            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(1000))) {
                    log.info("Received: topic={} partition={} offset={} value={}",
                            record.topic(), record.partition(), record.offset(), record.value());
                }
            }
        }
    }
}
