package com.mateibitache.minikafka.service;

import com.mateibitache.minikafka.error.ApiException;
import com.mateibitache.minikafka.model.Topic;
import com.mateibitache.minikafka.storage.TopicStore;
import jakarta.annotation.PostConstruct;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TopicService {

    private final TopicStore topicStore;
    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    public TopicService(TopicStore topicStore) {
        this.topicStore = topicStore;
    }

    @PostConstruct
    public void loadTopics() {
        topicStore.loadTopics().forEach(topic -> topics.put(topic.name(), topic));
    }

    public synchronized Topic createTopic(String name, int partitions) {
        if (topics.containsKey(name)) {
            throw new ApiException(HttpStatus.CONFLICT, "Topic already exists");
        }
        Topic topic = new Topic(name, partitions);
        topicStore.saveTopic(topic);
        topics.put(name, topic);
        return topic;
    }

    public Topic getTopic(String name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "Topic not found");
        }
        return topic;
    }

    public List<Topic> listTopics() {
        return topics.values()
                .stream()
                .sorted(Comparator.comparing(Topic::name))
                .toList();
    }
}
