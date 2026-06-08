package com.mateibitache.minikafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class EventStreamingApiTests {

    private static Path storagePath;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception {
        storagePath = Files.createTempDirectory("mini-kafka-test-");
        registry.add("mini-kafka.storage-dir", () -> storagePath.toString());
    }

    @Test
    void createsTopicPublishesAndConsumesWithStoredOffsets() throws Exception {
        createTopic("orders", 2);
        publish("orders", "a", "created", 0);
        publish("orders", "b", "paid", 0);

        mockMvc.perform(get("/api/consumer-groups/orders-service/topics/orders/messages")
                        .param("maxMessages", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messages.length()").value(1))
                .andExpect(jsonPath("$.messages[0].offset").value(0))
                .andExpect(jsonPath("$.messages[0].value").value("created"));

        mockMvc.perform(get("/api/consumer-groups/orders-service/topics/orders/messages")
                        .param("maxMessages", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messages.length()").value(1))
                .andExpect(jsonPath("$.messages[0].offset").value(1))
                .andExpect(jsonPath("$.messages[0].value").value("paid"));

        mockMvc.perform(get("/api/consumer-groups/analytics/topics/orders/messages")
                        .param("maxMessages", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messages.length()").value(2))
                .andExpect(jsonPath("$.messages[0].offset").value(0));

        Path logFile = storagePath.resolve("topics").resolve("orders").resolve("partition-0.log");
        Path offsetFile = storagePath.resolve("offsets").resolve("orders-service").resolve("orders").resolve("partition-0.offset");

        assertThat(Files.readAllLines(logFile)).hasSize(2);
        assertThat(Files.readString(offsetFile)).isEqualTo("2");
    }

    @Test
    void rejectsInvalidTopicAndUnknownPartition() throws Exception {
        mockMvc.perform(post("/api/topics")
                        .contentType("application/json")
                        .content("""
                                {"name":"bad topic","partitions":0}
                                """))
                .andExpect(status().isBadRequest());

        createTopic("payments", 1);

        mockMvc.perform(post("/api/topics/payments/messages")
                        .contentType("application/json")
                        .content("""
                                {"key":"p1","value":"created","partition":4}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void handlesConcurrentPublishingToSamePartition() throws Exception {
        createTopic("metrics", 1);

        int totalMessages = 30;
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(6);
        List<Exception> failures = new ArrayList<>();

        for (int i = 0; i < totalMessages; i++) {
            int index = i;
            executor.submit(() -> {
                try {
                    start.await(5, TimeUnit.SECONDS);
                    publish("metrics", "worker", "value-" + index, 0);
                } catch (Exception exception) {
                    synchronized (failures) {
                        failures.add(exception);
                    }
                }
            });
        }

        start.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(failures).isEmpty();

        String response = mockMvc.perform(get("/api/consumer-groups/checker/topics/metrics/messages")
                        .param("maxMessages", "100"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messages.length()").value(totalMessages))
                .andReturn()
                .getResponse()
                .getContentAsString();

        JsonNode messages = objectMapper.readTree(response).get("messages");
        HashSet<Long> offsets = new HashSet<>();
        for (JsonNode message : messages) {
            offsets.add(message.get("offset").asLong());
        }

        assertThat(offsets).hasSize(totalMessages);
        assertThat(offsets).contains(0L, 29L);
    }

    private void createTopic(String name, int partitions) throws Exception {
        mockMvc.perform(post("/api/topics")
                        .contentType("application/json")
                        .content("""
                                {"name":"%s","partitions":%d}
                                """.formatted(name, partitions)))
                .andExpect(status().isCreated());
    }

    private void publish(String topic, String key, String value, int partition) throws Exception {
        mockMvc.perform(post("/api/topics/{topic}/messages", topic)
                        .contentType("application/json")
                        .content("""
                                {"key":"%s","value":"%s","partition":%d}
                                """.formatted(key, value, partition)))
                .andExpect(status().isCreated());
    }
}
