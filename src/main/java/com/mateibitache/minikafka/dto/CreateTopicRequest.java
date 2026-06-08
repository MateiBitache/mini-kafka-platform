package com.mateibitache.minikafka.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CreateTopicRequest(
        @NotBlank
        @Pattern(regexp = "^[a-zA-Z0-9._-]+$")
        String name,

        @Min(1)
        @Max(12)
        int partitions
) {
}
