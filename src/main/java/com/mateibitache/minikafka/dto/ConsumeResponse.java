package com.mateibitache.minikafka.dto;

import java.util.List;

public record ConsumeResponse(
        String group,
        String topic,
        List<MessageResponse> messages
) {
}
