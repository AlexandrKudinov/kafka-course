package ru.practice.kafka.practical3.model;

import java.util.List;

public record BlockedUsers(List<String> userIds) {
}
