package ru.practice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.practice.kafka.model.Message;

@Slf4j
@Service
@RequiredArgsConstructor
public class SingleMessageConsumer {

    private final ObjectMapper mapper;

    @KafkaListener(
            topics = "${kafka.topic}",
            groupId = "${kafka.consumers.single.group-id}",
            containerFactory = "singleKafkaListenerContainerFactory"
    )
    public void consume(String payload) {
        try {
            Message message = mapper.readValue(payload, Message.class);
            log.info("Single consumer received: {}", message);
            process(message);
        } catch (Exception e) {
            log.error("SingleMessageConsumer error. Payload: {}", payload, e);
        }
    }

    private void process(Message message) {
        log.info("Single consumer processed message: {}", message.getId());
    }
}
