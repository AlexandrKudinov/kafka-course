package ru.practice.kafka.practical6.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SecurityProducer implements CommandLineRunner {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.producer.topic}")
    private String topic;

    @Override
    public void run(String... args) {
        kafkaTemplate.send(topic, "ssl-message-1")
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Failed to send message to {}", topic, error);
                        return;
                    }
                    log.info("Message sent to {} partition={}, offset={}", topic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                });
    }
}
