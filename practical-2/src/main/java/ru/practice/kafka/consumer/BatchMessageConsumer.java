package ru.practice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.practice.kafka.model.Message;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchMessageConsumer {

    private final ObjectMapper mapper;

    @KafkaListener(
            topics = "${kafka.topic}",
            groupId = "${kafka.consumers.batch.group-id}",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consume(List<String> payloads) {
        try {
            log.info("Batch consumer received {} messages", payloads.size());

            for (String payload : payloads) {
                Message message = mapper.readValue(payload, Message.class);
                log.info("Batch consumer received message: {}", message);
                process(message);
            }

            log.info("Batch processed: {} messages", payloads.size());
        } catch (Exception e) {
            log.error("BatchMessageConsumer error", e);
        }
    }

    private void process(Message message) {
        log.info("Batch consumer processed message: {}", message.getId());
    }
}
