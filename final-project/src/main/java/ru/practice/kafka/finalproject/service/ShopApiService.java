package ru.practice.kafka.finalproject.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.practice.kafka.finalproject.model.Product;

import java.io.File;

@Slf4j
@Service
public class ShopApiService {
    private final ObjectMapper mapper;
    private final KafkaClientFactory clientFactory;
    private final KafkaProperties kafka;
    private final String dataFile;

    public ShopApiService(ObjectMapper mapper, KafkaClientFactory clientFactory, KafkaProperties kafka,
                          @Value("${shop-api.products-file}") String dataFile) {
        this.mapper = mapper;
        this.clientFactory = clientFactory;
        this.kafka = kafka;
        this.dataFile = dataFile;
    }

    public void publishProducts() throws Exception {
        Product[] products = mapper.readValue(new File(dataFile), Product[].class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                clientFactory.producerProperties(kafka.primaryBootstrap, kafka.shopKeystore))) {
            for (Product product : products) {
                String json = mapper.writeValueAsString(product);
                producer.send(new ProducerRecord<>(kafka.productsTopic, product.getProductId(), json), (metadata, error) -> {
                    if (error != null) {
                        log.error("SHOP API failed to send product {}", product.getProductId(), error);
                    } else {
                        log.info("SHOP API sent productId={} partition={} offset={}", product.getProductId(), metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        }
        log.info("SHOP API published {} products from {}", products.length, dataFile);
    }
}
