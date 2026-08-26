package ru.practice.kafka.practical7;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
public class KafkaMessageProducer {

    private final ObjectMapper objectMapper;
    private final String topic;
    private final String bootstrapServers;

    public KafkaMessageProducer(
            @Value("${kafka.topic}") String topic,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers) {
        this.objectMapper = new ObjectMapper();
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
    }

    public void sendTestMessages() throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 1; i <= 10; i++) {
                Map<String, Object> message = new LinkedHashMap<>();
                message.put("id", i);
                message.put("name", "user-" + i);
                message.put("value", "message-" + i);

                String json = objectMapper.writeValueAsString(message);
                producer.send(new ProducerRecord<>(topic, String.valueOf(i), json),
                        (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Producer error: {}", exception.getMessage());
                            } else {
                                log.info("Sent: topic={} partition={} offset={} value={}",
                                        metadata.topic(), metadata.partition(), metadata.offset(), json);
                            }
                        });
            }
            producer.flush();
        }
    }
}
