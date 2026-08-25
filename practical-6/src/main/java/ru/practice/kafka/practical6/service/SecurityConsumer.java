package ru.practice.kafka.practical6.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SecurityConsumer {

    @KafkaListener(
            topics = "${kafka.consumer.topic}",
            groupId = "${kafka.consumer.group-id}")
    public void consume(String message) {
        log.info("Received message: {}", message);
    }
}
