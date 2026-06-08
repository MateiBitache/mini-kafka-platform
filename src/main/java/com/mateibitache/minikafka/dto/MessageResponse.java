package com.mateibitache.minikafka.dto;

import java.time.Instant;

public record MessageResponse(
        String topic,
        int partition,
        long offset,
        String key,
        String value,
        Instant timestamp
) {
}
