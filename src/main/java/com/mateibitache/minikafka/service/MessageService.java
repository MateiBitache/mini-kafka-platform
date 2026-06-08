package com.mateibitache.minikafka.service;

import com.mateibitache.minikafka.error.ApiException;
import com.mateibitache.minikafka.model.StoredMessage;
import com.mateibitache.minikafka.model.Topic;
import com.mateibitache.minikafka.storage.MessageLogStore;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class MessageService {

    private final TopicService topicService;
    private final MessageLogStore messageLogStore;
    private final ConcurrentHashMap<String, AtomicInteger> roundRobinCounters = new ConcurrentHashMap<>();

    public MessageService(TopicService topicService, MessageLogStore messageLogStore) {
        this.topicService = topicService;
        this.messageLogStore = messageLogStore;
    }

    public StoredMessage publish(String topicName, String key, String value, Integer requestedPartition) {
        Topic topic = topicService.getTopic(topicName);
        int partition = choosePartition(topic, key, requestedPartition);
        return messageLogStore.append(topic.name(), partition, key, value);
    }

    private int choosePartition(Topic topic, String key, Integer requestedPartition) {
        if (requestedPartition != null) {
            if (requestedPartition >= topic.partitions()) {
                throw new ApiException(HttpStatus.BAD_REQUEST, "Partition does not exist for this topic");
            }
            return requestedPartition;
        }

        if (key != null && !key.isBlank()) {
            return Math.floorMod(key.hashCode(), topic.partitions());
        }

        AtomicInteger counter = roundRobinCounters.computeIfAbsent(topic.name(), ignored -> new AtomicInteger());
        return Math.floorMod(counter.getAndIncrement(), topic.partitions());
    }
}
