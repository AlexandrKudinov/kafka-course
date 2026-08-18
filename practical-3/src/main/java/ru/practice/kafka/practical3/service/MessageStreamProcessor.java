package ru.practice.kafka.practical3.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.practice.kafka.practical3.model.BannedWords;
import ru.practice.kafka.practical3.model.BlockedUsers;
import ru.practice.kafka.practical3.model.Message;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageStreamProcessor {

    private static final String BLOCKED = "BLOCKED";
    private final ObjectMapper objectMapper;

    public String filterByBlockedUser(String key, String messageJson, String blockedUsersJson) {
        if (messageJson == null) {
            return null;
        }

        try {
            Message message = objectMapper.readValue(messageJson, Message.class);
            BlockedUsers blockedUsers = parseBlockedUsers(blockedUsersJson);

            if (blockedUsers.userIds().contains(message.getSenderId())) {
                log.info("Message {} blocked: sender {} is blocked by recipient {}",
                        message.getId(), message.getSenderId(), message.getRecipientId());
                return BLOCKED;
            }

            return messageJson;
        } catch (JsonProcessingException e) {
            log.error("Unable to deserialize message: {}", messageJson, e);
            return null;
        }
    }

    public String applyCensorship(String key, String messageJson, String bannedWordsJson) {
        if (messageJson == null || BLOCKED.equals(messageJson)) {
            return null;
        }

        try {
            Message message = objectMapper.readValue(messageJson, Message.class);
            BannedWords bannedWords = parseBannedWords(bannedWordsJson);
            String censoredText = censor(message.getText(), bannedWords.words());

            if (!censoredText.equals(message.getText())) {
                message.setText(censoredText);
                log.info("Message {} was censored", message.getId());
            }

            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            log.error("Unable to process message: {}", messageJson, e);
            return null;
        }
    }

    private BlockedUsers parseBlockedUsers(String json) throws JsonProcessingException {
        if (json == null || json.isBlank()) {
            return new BlockedUsers(Collections.emptyList());
        }
        return objectMapper.readValue(json, BlockedUsers.class);
    }

    private BannedWords parseBannedWords(String json) throws JsonProcessingException {
        if (json == null || json.isBlank()) {
            return new BannedWords(Collections.emptyList());
        }
        return objectMapper.readValue(json, BannedWords.class);
    }

    private String censor(String text, List<String> words) {
        String result = text;
        for (String word : words) {
            if (word == null || word.isBlank()) {
                continue;
            }
            result = result.replaceAll(
                    "(?iu)\\b" + Pattern.quote(word.trim()) + "\\b",
                    "***"
            );
        }
        return result;
    }
}
