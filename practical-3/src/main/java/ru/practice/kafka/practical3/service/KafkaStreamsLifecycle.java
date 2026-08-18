package ru.practice.kafka.practical3.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaStreamsLifecycle implements SmartLifecycle {

    private final KafkaStreams kafkaStreams;
    private volatile boolean running;

    @Override
    public void start() {
        kafkaStreams.start();
        running = true;
        log.info("Kafka Streams application started");
    }

    @Override
    public void stop() {
        kafkaStreams.close();
        running = false;
        log.info("Kafka Streams application stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
