package com.mateibitache.minikafka.error;

import java.time.Instant;
import java.util.List;

public record ApiError(
        Instant timestamp,
        int status,
        String error,
        List<String> details
) {
}
