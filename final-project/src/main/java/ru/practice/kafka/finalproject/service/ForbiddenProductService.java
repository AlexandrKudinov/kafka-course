package ru.practice.kafka.finalproject.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ForbiddenProductService {
    private final KafkaClientFactory clientFactory;
    private final KafkaProperties kafka;

    public ForbiddenProductService(KafkaClientFactory clientFactory, KafkaProperties kafka) {
        this.clientFactory = clientFactory;
        this.kafka = kafka;
    }

    public void forbid(String productId) {
        send(productId, "forbidden");
    }

    public void allow(String productId) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                clientFactory.producerProperties(kafka.primaryBootstrap, kafka.streamsKeystore))) {
            producer.send(new ProducerRecord<>(kafka.forbiddenTopic, productId, null));
            producer.flush();
            log.info("Product {} removed from forbidden list", productId);
        }
    }

    private void send(String productId, String value) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                clientFactory.producerProperties(kafka.primaryBootstrap, kafka.streamsKeystore))) {
            producer.send(new ProducerRecord<>(kafka.forbiddenTopic, productId, value));
            producer.flush();
            log.info("Product {} added to forbidden list", productId);
        }
    }
}
