# Mini Kafka Event Streaming Platform

Mini Kafka Event Streaming Platform is a small Java Spring Boot project that shows the main ideas behind an event streaming system without trying to copy all of Kafka.

It exposes a REST API where producers can create topics and publish messages. Topics are split into partitions. Each partition is stored as an append-only log file on disk. Consumers read messages through a REST API, and the application stores offsets per consumer group so each group can continue from where it stopped.

## Why I Built It

I built this project to practice backend concepts that appear often in real systems:

- REST API design
- service and controller separation
- persistence without a database
- append-only logs
- consumer groups and offsets
- thread-safe publishing and consuming
- basic event-driven architecture ideas
- testing important backend behavior

The project is intentionally smaller than Kafka. The goal is to make the core ideas clear enough to explain in an interview.

## Main Features

- Create topics with one or more partitions
- Publish messages to a topic
- Select a partition explicitly, by key, or by round robin
- Store messages in append-only log files
- Consume messages by consumer group
- Persist offsets for each consumer group, topic, and partition
- Continue consuming from the saved offset after restart
- Basic validation and error responses
- Swagger UI through Springdoc OpenAPI
- Docker Compose setup
- Integration tests for the main flow and concurrent publishing

## Architecture

The application is organized into simple layers:

- `controller`: REST endpoints
- `service`: business logic for topics, messages, and consumers
- `storage`: file-based persistence for topics, logs, and offsets
- `model`: internal records used by the services
- `dto`: request and response objects for the API
- `error`: API error handling

There is no database. This is on purpose. Messages are stored in plain log files so the persistence model is easy to inspect and understand.

## API Overview

Swagger UI is available at:

```text
http://localhost:8080/swagger-ui/index.html
```

Main endpoints:

```text
POST /api/topics
GET  /api/topics
POST /api/topics/{topic}/messages
GET  /api/consumer-groups/{group}/topics/{topic}/messages
```

## Topics and Partitions

A topic is a named stream of messages. Each topic has a fixed number of partitions.

When a message is published:

- if a partition is provided, the message goes to that partition
- if a key is provided, the key hash chooses the partition
- if no key or partition is provided, the topic uses round robin

Each partition has its own log file, so messages in the same partition keep their offset order.

## Persistent Logs

Messages are stored under the configured storage directory:

```text
data/topics/{topic}/partition-{partition}.log
```

Each line in the log file is one JSON message. New messages are appended to the end of the file. Existing messages are not rewritten during normal publishing.

The offset of a message is its line position inside that partition log.

## Consumer Offsets

Each consumer group has its own offset files:

```text
data/offsets/{group}/{topic}/partition-{partition}.offset
```

The offset file stores the next message offset that the group should read. This means different consumer groups can read the same topic independently.

## Tech Stack

- Java 21
- Spring Boot 3
- Spring Web
- Jakarta Validation
- Springdoc OpenAPI
- Maven
- JUnit 5
- MockMvc
- Docker Compose

## Run Locally

Requirements:

- Java 21
- Maven 3.9+

Start the application:

```bash
mvn spring-boot:run
```

The API will run on:

```text
http://localhost:8080
```

## Run With Docker

```bash
docker compose up --build
```

Docker stores log and offset data in a named volume called `mini-kafka-data`.

## Run Tests

```bash
mvn test
```

## Example API Calls

Create a topic:

```bash
curl -X POST http://localhost:8080/api/topics \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"orders\",\"partitions\":3}"
```

Publish a message:

```bash
curl -X POST http://localhost:8080/api/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d "{\"key\":\"order-1\",\"value\":\"Order created\"}"
```

Consume messages:

```bash
curl "http://localhost:8080/api/consumer-groups/billing-service/topics/orders/messages?maxMessages=10"
```

Consume with another group:

```bash
curl "http://localhost:8080/api/consumer-groups/analytics/topics/orders/messages?maxMessages=10"
```

Both groups track their own offsets.

## What I Learned

- How Kafka-style topics, partitions, and offsets work at a basic level
- Why append-only logs are useful for event streaming
- How consumer groups can read the same data independently
- How to make file writes safe with locks
- How to design a small REST API around backend concepts
- How to test persistence and concurrency behavior

## Possible Future Improvements

- Add message retention by age or log size
- Add offset reset support
- Add topic deletion
- Add pagination-like polling per partition
- Add simple consumer group metadata endpoints
- Add metrics for published and consumed messages
- Add a small UI for viewing topics and logs
