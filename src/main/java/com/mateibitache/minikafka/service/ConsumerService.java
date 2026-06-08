package com.mateibitache.minikafka.service;

import com.mateibitache.minikafka.model.StoredMessage;
import com.mateibitache.minikafka.model.Topic;
import com.mateibitache.minikafka.storage.MessageLogStore;
import com.mateibitache.minikafka.storage.OffsetStore;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ConsumerService {

    private final TopicService topicService;
    private final MessageLogStore messageLogStore;
    private final OffsetStore offsetStore;

    public ConsumerService(TopicService topicService, MessageLogStore messageLogStore, OffsetStore offsetStore) {
        this.topicService = topicService;
        this.messageLogStore = messageLogStore;
        this.offsetStore = offsetStore;
    }

    public List<StoredMessage> consume(String group, String topicName, int maxMessages) {
        Topic topic = topicService.getTopic(topicName);
        List<StoredMessage> result = new ArrayList<>();

        for (int partition = 0; partition < topic.partitions() && result.size() < maxMessages; partition++) {
            int remaining = maxMessages - result.size();
            long currentOffset = offsetStore.readOffset(group, topic.name(), partition);
            List<StoredMessage> messages = messageLogStore.read(topic.name(), partition, currentOffset, remaining);
            if (!messages.isEmpty()) {
                StoredMessage lastMessage = messages.getLast();
                offsetStore.saveOffset(group, topic.name(), partition, lastMessage.offset() + 1);
                result.addAll(messages);
            }
        }

        return result;
    }
}
