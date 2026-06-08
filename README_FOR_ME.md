# README FOR ME

This file explains the project in a practical way so I can understand it and talk about it in an interview.

## Project Structure

```text
mini-kafka-platform/
  Dockerfile
  docker-compose.yml
  pom.xml
  README.md
  README_FOR_ME.md
  src/
    main/
      java/
        com/mateibitache/minikafka/
          MiniKafkaPlatformApplication.java
          config/
            AppConfig.java
            OpenApiConfig.java
            StorageProperties.java
          controller/
            MessageController.java
            TopicController.java
          dto/
            ConsumeResponse.java
            CreateTopicRequest.java
            MessageResponse.java
            PublishMessageRequest.java
            TopicResponse.java
          error/
            ApiError.java
            ApiException.java
            GlobalExceptionHandler.java
          model/
            StoredMessage.java
            Topic.java
          service/
            ConsumerService.java
            MessageService.java
            TopicService.java
          storage/
            MessageLogStore.java
            OffsetStore.java
            TopicStore.java
      resources/
        application.yml
    test/
      java/
        com/mateibitache/minikafka/
          EventStreamingApiTests.java
          MiniKafkaPlatformApplicationTests.java
```

## What The Important Files Do

`MiniKafkaPlatformApplication.java`

Starts the Spring Boot application.

`application.yml`

Sets the app name, server port, and storage directory. By default the app writes to `data`.

`StorageProperties.java`

Reads the `mini-kafka.storage-dir` config value so storage classes know where to write files.

`TopicController.java`

Handles topic API endpoints:

- create a topic
- list topics

`MessageController.java`

Handles message API endpoints:

- publish a message to a topic
- consume messages for a consumer group

`TopicService.java`

Keeps topic metadata in memory and saves new topics to disk.

`MessageService.java`

Chooses the partition for a new message and sends it to the log store.

`ConsumerService.java`

Reads the current offset for a consumer group, reads messages from logs, and updates offsets after consuming.

`TopicStore.java`

Persists topic metadata in `topic.json` and creates the partition log files.

`MessageLogStore.java`

Appends messages to partition log files and reads messages back from a given offset.

`OffsetStore.java`

Stores and reads consumer group offsets from small text files.

`GlobalExceptionHandler.java`

Turns validation errors and custom API errors into JSON responses.

`EventStreamingApiTests.java`

Tests the main behavior through the REST API with MockMvc.

## How A Message Moves Through The System

1. A producer sends a request to:

```text
POST /api/topics/{topic}/messages
```

2. `MessageController` receives the request and validates the body.

3. `MessageService` checks that the topic exists.

4. `MessageService` chooses a partition.

5. `MessageLogStore` opens the correct partition log file.

6. The message gets an offset based on the next line number for that partition.

7. The message is written as one JSON line at the end of the log file.

8. The API returns the topic, partition, offset, key, value, and timestamp.

## How Partitions Are Selected

There are three cases:

1. If the request has a `partition`, the app uses that partition.
2. If the request has a `key`, the app hashes the key and maps it to a partition.
3. If there is no partition and no key, the app uses round robin for that topic.

This is similar to Kafka at a basic level. The same key should keep going to the same partition as long as the topic has the same partition count.

## How Offsets Are Stored And Updated

Offsets are stored per consumer group, topic, and partition.

Example:

```text
data/offsets/billing-service/orders/partition-0.offset
```

The file stores the next offset to read.

If the file contains:

```text
2
```

then the next consume request starts from offset `2`.

When a group consumes messages, the app updates the offset to the last consumed offset plus one.

Different groups have different offset files. That is why `billing-service` and `analytics` can both read the same topic without affecting each other.

## How Persistence Works

Topic metadata is stored here:

```text
data/topics/{topic}/topic.json
```

Messages are stored here:

```text
data/topics/{topic}/partition-{partition}.log
```

Each log file is append-only. Each line is one JSON message.

On startup, `TopicService` loads existing topic metadata from disk. When a new message is published, `MessageLogStore` counts the existing lines for that partition the first time it sees the file. That count becomes the next offset.

Because the logs and offsets are on disk, the app can restart and continue with the same data.

## How Concurrency Is Handled

The app uses simple locks around file access.

`MessageLogStore` has a read-write lock for each partition log file:

- publishing uses the write lock
- consuming uses the read lock

This keeps two publishers from writing the same offset at the same time.

`OffsetStore` also locks each offset file while reading or writing it.

This is not a distributed locking system. It is enough for one Spring Boot application process and keeps the code understandable.

## What I Learned

- A topic is a named stream of messages.
- A partition is an ordered log inside a topic.
- An offset is the position of a message inside one partition.
- A consumer group needs its own offsets so it can continue later.
- Append-only files are simple but powerful for event streaming.
- Concurrency matters even in a small backend project.
- Tests are useful when code touches both REST APIs and files.

## What Was Hardest

The hardest part was connecting offsets to partitions correctly.

At first it is easy to think a topic has one offset, but the offset really belongs to a partition. A topic with three partitions needs three offsets for each consumer group.

The other tricky part was concurrent publishing. If two requests publish to the same partition at the same time, they must not get the same offset. The write lock in `MessageLogStore` prevents that.

## How To Explain It In An Interview

A short version:

```text
I built a simplified Kafka-like event streaming platform in Spring Boot. Producers publish messages to topics through REST APIs. Topics have partitions, and each partition is stored as an append-only JSON log file on disk. Consumers read messages by consumer group, and the app stores offsets per group, topic, and partition so each group can resume where it left off. I used locks around log and offset files to handle concurrent requests safely, and I added tests for publishing, consuming, persistence, validation, and concurrent writes.
```

A longer version:

```text
The project is not meant to replace Kafka. I built it to understand the ideas behind Kafka: logs, partitions, offsets, and consumer groups. The storage is file-based so the data model is visible. A message gets written to a partition log, and the offset is its position in that log. When a consumer group polls messages, the app reads from the saved offset and then stores the next offset. This makes the system restart-friendly and lets different consumer groups read independently.
```

## Possible Interview Questions And Answers

Question: Why did you use append-only logs?

Answer: Append-only logs are simple and reliable for event streaming. New messages are added at the end, so the existing history is not changed during normal publishing. This also makes offsets easy because the offset is the message position inside the partition.

Question: What is a partition?

Answer: A partition is an ordered log inside a topic. A topic can have multiple partitions so messages can be spread out. Ordering is guaranteed inside one partition, not across the whole topic.

Question: How does the app choose a partition?

Answer: If the producer sends a partition, the app uses it. If the producer sends a key, the app hashes the key so the same key goes to the same partition. If neither is provided, it uses round robin.

Question: What is a consumer group offset?

Answer: It is the next message position a consumer group should read for a specific topic partition. It lets the group continue later without reading old messages again.

Question: Why are offsets stored per partition?

Answer: Because each partition has its own ordered log. Offset `5` in partition `0` is different from offset `5` in partition `1`.

Question: How does persistence survive a restart?

Answer: Topic metadata, partition logs, and offset files are stored on disk. On startup, the app loads topics from `topic.json` files. When publishing resumes, it counts the existing log lines to know the next offset.

Question: How did you handle concurrency?

Answer: Each partition log has a read-write lock. Publishing takes the write lock so offsets and appends are safe. Consuming takes the read lock. Offset files also have simple synchronized locks.

Question: What are the limits of this project?

Answer: It runs as one application process, does not replicate data, does not rebalance consumers, and does not implement Kafka's network protocol. It is a learning project focused on the core ideas.

Question: What would you improve next?

Answer: I would add retention policies, offset reset, topic deletion, metrics, and maybe a small UI to inspect topics and partitions.
