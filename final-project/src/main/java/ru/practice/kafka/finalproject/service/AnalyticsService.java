package ru.practice.kafka.finalproject.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Service
public class AnalyticsService {
    private final KafkaClientFactory factory;
    private final KafkaProperties kafka;
    private final ObjectMapper mapper;

    public AnalyticsService(KafkaClientFactory factory, KafkaProperties kafka, ObjectMapper mapper) {
        this.factory = factory;
        this.kafka = kafka;
        this.mapper = mapper;
    }

    public void startAndWait() throws Exception {
        Properties filteredProps = factory.plainConsumerProperties(
                kafka.secondaryBootstrap, "analytics-filtered");
        Properties requestsProps = factory.consumerProperties(
                kafka.primaryBootstrap, kafka.analyticsKeystore, "analytics-requests");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafka.primaryBootstrap);
        producerProps.put("security.protocol", "SSL");
        producerProps.put("ssl.truststore.location", kafka.truststore);
        producerProps.put("ssl.truststore.password", kafka.password);
        producerProps.put("ssl.keystore.location", kafka.analyticsKeystore);
        producerProps.put("ssl.keystore.password", kafka.password);
        producerProps.put("ssl.key.password", kafka.password);
        producerProps.put("ssl.endpoint.identification.algorithm", "https");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        Map<String, Integer> categoryCounts = new HashMap<>();
        try (KafkaConsumer<String, String> filteredConsumer = new KafkaConsumer<>(filteredProps);
             KafkaConsumer<String, String> requestConsumer = new KafkaConsumer<>(requestsProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            filteredConsumer.subscribe(Collections.singletonList(kafka.filteredTopic));
            requestConsumer.subscribe(Collections.singletonList(kafka.clientRequestsTopic));

            while (true) {
                for (ConsumerRecord<String, String> record : filteredConsumer.poll(Duration.ofMillis(250))) {
                    JsonNode node = mapper.readTree(record.value());
                    String category = node.path("category").asText("unknown");
                    categoryCounts.merge(category, 1, Integer::sum);
                }

                for (ConsumerRecord<String, String> record : requestConsumer.poll(Duration.ofMillis(250))) {
                    JsonNode request = mapper.readTree(record.value());
                    if (!"SEARCH".equals(request.path("type").asText())) {
                        continue;
                    }

                    JsonNode categories = request.path("categories");
                    if (!categories.isArray() || categories.isEmpty()) {
                        continue;
                    }

                    String category = categories.get(0).asText();
                    String productId = request.path("productIds").isArray()
                            && !request.path("productIds").isEmpty()
                            ? request.path("productIds").get(0).asText()
                            : "unknown";

                    String recommendation = mapper.createObjectNode()
                            .put("requestId", record.key())
                            .put("category", category)
                            .put("productId", productId)
                            .put("reason", "recommendation based on client search and filtered catalog")
                            .put("score", categoryCounts.getOrDefault(category, 0))
                            .toString();

                    producer.send(new ProducerRecord<>(kafka.recommendationsTopic, record.key(), recommendation));
                    log.info("Analytics produced recommendation requestId={} category={}", record.key(), category);
                }
                producer.flush();
                filteredConsumer.commitSync();
                requestConsumer.commitSync();
            }
        }
    }
}
