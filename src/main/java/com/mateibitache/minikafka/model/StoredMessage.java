package com.mateibitache.minikafka.model;

import java.time.Instant;

public record StoredMessage(
        String topic,
        int partition,
        long offset,
        String key,
        String value,
        Instant timestamp
) {
}
