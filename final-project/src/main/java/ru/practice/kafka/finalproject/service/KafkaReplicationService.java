package ru.practice.kafka.finalproject.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Service
public class KafkaReplicationService {
    private final KafkaClientFactory factory;
    private final KafkaProperties kafka;

    public KafkaReplicationService(KafkaClientFactory factory, KafkaProperties kafka) {
        this.factory = factory;
        this.kafka = kafka;
    }

    public void startAndWait() {
        Properties consumerProps = factory.consumerProperties(kafka.primaryBootstrap, kafka.streamsKeystore, "final-project-replicator");
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafka.secondaryBootstrap);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("acks", "all");
        producerProps.put("enable.idempotence", "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            consumer.subscribe(Collections.singletonList(kafka.filteredTopic));
            while (true) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                    producer.send(new ProducerRecord<>(kafka.filteredTopic, record.key(), record.value()));
                }
                producer.flush();
                consumer.commitSync();
            }
        }
    }
}
