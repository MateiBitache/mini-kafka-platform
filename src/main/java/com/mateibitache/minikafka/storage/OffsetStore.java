package com.mateibitache.minikafka.storage;

import com.mateibitache.minikafka.config.StorageProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OffsetStore {

    private final Path offsetsPath;
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    public OffsetStore(StorageProperties storageProperties) {
        this.offsetsPath = Path.of(storageProperties.getStorageDir()).resolve("offsets");
    }

    public long readOffset(String group, String topic, int partition) {
        Path path = offsetPath(group, topic, partition);
        Object lock = lockFor(path);
        synchronized (lock) {
            if (!Files.exists(path)) {
                return 0;
            }
            try {
                String value = Files.readString(path, StandardCharsets.UTF_8).trim();
                return value.isBlank() ? 0 : Long.parseLong(value);
            } catch (IOException exception) {
                throw new IllegalStateException("Could not read consumer offset", exception);
            }
        }
    }

    public void saveOffset(String group, String topic, int partition, long offset) {
        Path path = offsetPath(group, topic, partition);
        Object lock = lockFor(path);
        synchronized (lock) {
            try {
                Files.createDirectories(path.getParent());
                Files.writeString(path, String.valueOf(offset), StandardCharsets.UTF_8);
            } catch (IOException exception) {
                throw new IllegalStateException("Could not save consumer offset", exception);
            }
        }
    }

    private Object lockFor(Path path) {
        return locks.computeIfAbsent(path.toString(), ignored -> new Object());
    }

    private Path offsetPath(String group, String topic, int partition) {
        return offsetsPath
                .resolve(group)
                .resolve(topic)
                .resolve("partition-" + partition + ".offset");
    }
}
