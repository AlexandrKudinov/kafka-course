package ru.practice.kafka.practical3.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    private String id;
    private String senderId;
    private String recipientId;
    private String text;
}
