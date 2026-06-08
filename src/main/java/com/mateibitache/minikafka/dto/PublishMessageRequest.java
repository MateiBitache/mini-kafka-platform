package com.mateibitache.minikafka.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record PublishMessageRequest(
        String key,

        @NotBlank
        String value,

        @Min(0)
        Integer partition
) {
}
