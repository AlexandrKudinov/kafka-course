package ru.practice.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.practice.kafka.model.Message;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    @Value("${kafka.topic}")
    private String topic;


    public void send(String text) {
        Message message = new Message(
                UUID.randomUUID().toString(),
                text,
                System.currentTimeMillis()
        );

        try {
            String json = mapper.writeValueAsString(message);
            log.info("Sending message: {}", json);

            kafkaTemplate.send(topic, message.getId(), json)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.error("Producer error for message {}", message.getId(), error);
                            return;
                        }

                        var metadata = result.getRecordMetadata();
                        log.info(
                                "Message sent: topic={}, partition={}, offset={}",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset()
                        );
                    });
        } catch (Exception e) {
            log.error("Serialization error for message {}", message.getId(), e);
        }
    }
}
