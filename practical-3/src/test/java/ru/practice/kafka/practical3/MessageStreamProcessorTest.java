package ru.practice.kafka.practical3;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import ru.practice.kafka.practical3.service.MessageStreamProcessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStreamProcessorTest {

    private final MessageStreamProcessor processor = new MessageStreamProcessor(new ObjectMapper());

    @Test
    void shouldBlockMessageFromBlockedSender() {
        String message = "{\"id\":\"1\",\"senderId\":\"user-2\",\"recipientId\":\"user-1\",\"text\":\"hello\"}";
        String blockedUsers = "{\"userIds\":[\"user-2\"]}";

        assertEquals("BLOCKED", processor.filterByBlockedUser("user-1", message, blockedUsers));
    }

    @Test
    void shouldCensorBannedWords() {
        String message = "{\"id\":\"1\",\"senderId\":\"user-2\",\"recipientId\":\"user-1\",\"text\":\"hello bad word\"}";
        String bannedWords = "{\"words\":[\"bad\"]}";

        String result = processor.applyCensorship("global", message, bannedWords);

        assertNotNull(result);
        assertTrue(result.contains("hello *** word"));
    }

    @Test
    void shouldPassMessageWithoutRestrictions() {
        String message = "{\"id\":\"1\",\"senderId\":\"user-2\",\"recipientId\":\"user-1\",\"text\":\"hello\"}";

        assertEquals(message, processor.filterByBlockedUser("user-1", message, null));
    }
}
