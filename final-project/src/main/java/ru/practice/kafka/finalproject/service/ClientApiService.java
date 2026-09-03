package ru.practice.kafka.finalproject.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.practice.kafka.finalproject.model.Product;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class ClientApiService {
    private final KafkaClientFactory factory;
    private final KafkaProperties kafka;
    private final ObjectMapper mapper;
    private final String dataFile;

    public ClientApiService(KafkaClientFactory factory, KafkaProperties kafka, ObjectMapper mapper,
                            @Value("${shop-api.products-file}") String dataFile) {
        this.factory = factory;
        this.kafka = kafka;
        this.mapper = mapper;
        this.dataFile = dataFile;
    }

    public void search(String query) throws Exception {
        Product[] products = mapper.readValue(new File(dataFile), Product[].class);
        List<Product> matches = Arrays.stream(products)
                .filter(product -> product.getName() != null
                        && product.getName().toLowerCase().contains(query.toLowerCase()))
                .toList();

        List<String> productIds = matches.stream().map(Product::getProductId).toList();
        List<String> categories = matches.stream().map(Product::getCategory).distinct().toList();

        String requestId = UUID.randomUUID().toString();
        ObjectNode request = mapper.createObjectNode()
                .put("type", "SEARCH")
                .put("query", query)
                .put("requestId", requestId)
                .set("productIds", mapper.valueToTree(productIds));
        request.set("categories", mapper.valueToTree(categories));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                factory.producerProperties(kafka.primaryBootstrap, kafka.clientKeystore))) {
            producer.send(new ProducerRecord<>(kafka.clientRequestsTopic, requestId, request.toString()));
            producer.flush();
        }

        log.info("CLIENT API search query={} matches={} requestId={}", query, productIds.size(), requestId);
    }

    public void recommend(String category) throws Exception {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                factory.consumerProperties(kafka.primaryBootstrap, kafka.clientKeystore,
                        "client-recommendations-" + UUID.randomUUID()))) {
            consumer.subscribe(Collections.singletonList(kafka.recommendationsTopic));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value().toLowerCase().contains("\"category\":\"" + category.toLowerCase() + "\"")) {
                        log.info("CLIENT API received ready recommendation: {}", record.value());
                        return;
                    }
                }
            }
            log.info("CLIENT API did not find a ready recommendation for category={}", category);
        }
    }
}
