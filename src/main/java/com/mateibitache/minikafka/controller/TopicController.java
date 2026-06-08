package com.mateibitache.minikafka.controller;

import com.mateibitache.minikafka.dto.CreateTopicRequest;
import com.mateibitache.minikafka.dto.TopicResponse;
import com.mateibitache.minikafka.model.Topic;
import com.mateibitache.minikafka.service.TopicService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/topics")
public class TopicController {

    private final TopicService topicService;

    public TopicController(TopicService topicService) {
        this.topicService = topicService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public TopicResponse createTopic(@Valid @RequestBody CreateTopicRequest request) {
        return toResponse(topicService.createTopic(request.name(), request.partitions()));
    }

    @GetMapping
    public List<TopicResponse> listTopics() {
        return topicService.listTopics()
                .stream()
                .map(this::toResponse)
                .toList();
    }

    private TopicResponse toResponse(Topic topic) {
        return new TopicResponse(topic.name(), topic.partitions());
    }
}
