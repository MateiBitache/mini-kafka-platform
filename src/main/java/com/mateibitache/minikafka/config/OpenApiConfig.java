package com.mateibitache.minikafka.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Mini Kafka Event Streaming Platform")
                        .version("1.0.0")
                        .description("A small REST-based event streaming system with topics, partitions, logs, and consumer offsets."));
    }
}
