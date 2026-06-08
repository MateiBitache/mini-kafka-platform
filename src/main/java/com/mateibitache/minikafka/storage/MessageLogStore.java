package com.mateibitache.minikafka.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mateibitache.minikafka.config.StorageProperties;
import com.mateibitache.minikafka.model.StoredMessage;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

@Component
public class MessageLogStore {

    private final ObjectMapper objectMapper;
    private final Path topicsPath;
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> nextOffsets = new ConcurrentHashMap<>();

    public MessageLogStore(ObjectMapper objectMapper, StorageProperties storageProperties) {
        this.objectMapper = objectMapper;
        this.topicsPath = Path.of(storageProperties.getStorageDir()).resolve("topics");
    }

    public StoredMessage append(String topic, int partition, String key, String value) {
        Path logPath = logPath(topic, partition);
        ReentrantReadWriteLock.WriteLock lock = lockFor(logPath).writeLock();
        lock.lock();
        try {
            Files.createDirectories(logPath.getParent());
            long offset = nextOffsets.computeIfAbsent(logPath.toString(), ignored -> countLines(logPath));
            StoredMessage message = new StoredMessage(topic, partition, offset, key, value, Instant.now());
            String line = objectMapper.writeValueAsString(message) + System.lineSeparator();
            Files.writeString(logPath, line, StandardCharsets.UTF_8, CREATE, APPEND);
            nextOffsets.put(logPath.toString(), offset + 1);
            return message;
        } catch (IOException exception) {
            throw new IllegalStateException("Could not append message", exception);
        } finally {
            lock.unlock();
        }
    }

    public List<StoredMessage> read(String topic, int partition, long offset, int maxMessages) {
        Path logPath = logPath(topic, partition);
        if (!Files.exists(logPath) || maxMessages <= 0) {
            return List.of();
        }

        ReentrantReadWriteLock.ReadLock lock = lockFor(logPath).readLock();
        lock.lock();
        try (Stream<String> lines = Files.lines(logPath, StandardCharsets.UTF_8)) {
            return lines.skip(offset)
                    .limit(maxMessages)
                    .map(this::readLine)
                    .toList();
        } catch (IOException exception) {
            throw new IllegalStateException("Could not read messages", exception);
        } finally {
            lock.unlock();
        }
    }

    private StoredMessage readLine(String line) {
        try {
            return objectMapper.readValue(line, StoredMessage.class);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not parse message log line", exception);
        }
    }

    private long countLines(Path logPath) {
        if (!Files.exists(logPath)) {
            return 0;
        }

        try (Stream<String> lines = Files.lines(logPath, StandardCharsets.UTF_8)) {
            return lines.count();
        } catch (IOException exception) {
            throw new IllegalStateException("Could not read log size", exception);
        }
    }

    private ReentrantReadWriteLock lockFor(Path path) {
        return locks.computeIfAbsent(path.toString(), ignored -> new ReentrantReadWriteLock());
    }

    private Path logPath(String topic, int partition) {
        return topicsPath.resolve(topic).resolve("partition-" + partition + ".log");
    }
}
