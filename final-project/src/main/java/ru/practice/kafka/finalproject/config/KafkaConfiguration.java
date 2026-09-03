package ru.practice.kafka.finalproject.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.practice.kafka.finalproject.service.KafkaClientFactory;
import ru.practice.kafka.finalproject.service.KafkaProperties;

@Configuration
public class KafkaConfiguration {

    @Bean
    public KafkaClientFactory kafkaClientFactory(KafkaProperties kafkaProperties) {
        return new KafkaClientFactory(kafkaProperties);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
