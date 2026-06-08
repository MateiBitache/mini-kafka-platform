package com.mateibitache.minikafka.controller;

import com.mateibitache.minikafka.dto.ConsumeResponse;
import com.mateibitache.minikafka.dto.MessageResponse;
import com.mateibitache.minikafka.dto.PublishMessageRequest;
import com.mateibitache.minikafka.model.StoredMessage;
import com.mateibitache.minikafka.service.ConsumerService;
import com.mateibitache.minikafka.service.MessageService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Validated
@RestController
@RequestMapping("/api")
public class MessageController {

    private final MessageService messageService;
    private final ConsumerService consumerService;

    public MessageController(MessageService messageService, ConsumerService consumerService) {
        this.messageService = messageService;
        this.consumerService = consumerService;
    }

    @PostMapping("/topics/{topic}/messages")
    @ResponseStatus(HttpStatus.CREATED)
    public MessageResponse publish(
            @PathVariable String topic,
            @Valid @RequestBody PublishMessageRequest request
    ) {
        StoredMessage message = messageService.publish(topic, request.key(), request.value(), request.partition());
        return toResponse(message);
    }

    @GetMapping("/consumer-groups/{group}/topics/{topic}/messages")
    public ConsumeResponse consume(
            @PathVariable @Pattern(regexp = "^[a-zA-Z0-9._-]+$") String group,
            @PathVariable String topic,
            @RequestParam(defaultValue = "10") @Min(1) @Max(100) int maxMessages
    ) {
        List<MessageResponse> messages = consumerService.consume(group, topic, maxMessages)
                .stream()
                .map(this::toResponse)
                .toList();
        return new ConsumeResponse(group, topic, messages);
    }

    private MessageResponse toResponse(StoredMessage message) {
        return new MessageResponse(
                message.topic(),
                message.partition(),
                message.offset(),
                message.key(),
                message.value(),
                message.timestamp()
        );
    }
}
