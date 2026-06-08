package com.mateibitache.minikafka.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mateibitache.minikafka.config.StorageProperties;
import com.mateibitache.minikafka.model.Topic;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Component
public class TopicStore {

    private static final String TOPIC_FILE = "topic.json";

    private final ObjectMapper objectMapper;
    private final Path topicsPath;

    public TopicStore(ObjectMapper objectMapper, StorageProperties storageProperties) {
        this.objectMapper = objectMapper;
        this.topicsPath = Path.of(storageProperties.getStorageDir()).resolve("topics");
    }

    public List<Topic> loadTopics() {
        if (!Files.exists(topicsPath)) {
            return List.of();
        }

        try (var paths = Files.list(topicsPath)) {
            List<Topic> topics = new ArrayList<>();
            for (Path topicPath : paths.filter(Files::isDirectory).toList()) {
                Path metadataPath = topicPath.resolve(TOPIC_FILE);
                if (Files.exists(metadataPath)) {
                    topics.add(objectMapper.readValue(metadataPath.toFile(), Topic.class));
                }
            }
            return topics;
        } catch (IOException exception) {
            throw new IllegalStateException("Could not load topics", exception);
        }
    }

    public void saveTopic(Topic topic) {
        try {
            Path topicPath = topicsPath.resolve(topic.name());
            Files.createDirectories(topicPath);
            objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValue(topicPath.resolve(TOPIC_FILE).toFile(), topic);
            for (int partition = 0; partition < topic.partitions(); partition++) {
                Path logPath = topicPath.resolve("partition-" + partition + ".log");
                if (!Files.exists(logPath)) {
                    Files.createFile(logPath);
                }
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Could not save topic " + topic.name(), exception);
        }
    }
}
