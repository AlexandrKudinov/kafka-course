package ru.practice.kafka.practical5;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
public class KafkaCdcConsumer implements CommandLineRunner {
    private final ObjectMapper objectMapper;
    private final String bootstrapServers;
    private final String groupId;

    public KafkaCdcConsumer(@Value("${kafka.bootstrap-servers}") String bootstrapServers,
                            @Value("${kafka.consumer.group-id}") String groupId) {
        this.objectMapper = new ObjectMapper();
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    @Override
    public void run(String... args) {
        Thread thread = new Thread(this::consume, "cdc-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    private void consume() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of("practical5.public.users", "practical5.public.orders"));
            while (!Thread.currentThread().isInterrupted()) {
                consumer.poll(Duration.ofSeconds(1)).forEach(r -> printEvent(r.topic(), r.value()));
            }
        } catch (Exception e) {
            log.error("CDC consumer stopped", e);
        }
    }

    private void printEvent(String topic, String value) {
        try {
            JsonNode event = objectMapper.readTree(value);
            log.info("CDC event: topic={}, payload={}", topic, event);
        } catch (Exception e) {
            log.error("Cannot parse CDC event: {}", value, e);
        }
    }
}
