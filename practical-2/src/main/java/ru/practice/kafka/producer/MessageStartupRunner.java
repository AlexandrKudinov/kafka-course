package ru.practice.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MessageStartupRunner implements CommandLineRunner {

    private final MessageProducer producer;

    @Value("${kafka.test.messages-count}")
    private int messagesCount;

    public MessageStartupRunner(MessageProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run(String... args) {
        log.info("Starting test producer. Messages count: {}", messagesCount);

        for (int i = 1; i <= messagesCount; i++) {
            producer.send("message-" + i);
        }
    }
}
