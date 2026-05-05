package io.github.neobliz1.kafka.raft.scram.demo.base;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import io.github.neobliz1.kafka.raft.scram.demo.repository.OutboxRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Base test class for Kafka-related integration tests.
 * <p>
 * This abstract class provides a foundational setup for integration tests that involve Kafka.
 * It handles the lifecycle of Kafka topics, consumer setup, and provides utility methods
 * for interacting with the system under test. Subclasses are expected to provide
 * the specific Kafka bootstrap server and Schema Registry URLs.
 * </p>
 */
@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
public abstract class BaseKafkaTestCase {

    public static final String API_V_1_WEATHER = "/api/v1/weather";
    public static final String KAFKA = "kafka";
    public static final int KAFKA_PORT = 9092;
    public static final String SCHEMA_REGISTRY = "schema-registry";
    public static final int SCHEMA_REGISTRY_PORT = 8081;
    /**
     * Kafka consumer for verifying that messages are produced correctly.
     * This consumer is subscribed to the test topic and is reset before each test.
     */
    protected Consumer<String, WeatherPacket> consumer;

    @Autowired
    protected MockMvc mockMvc;

    /**
     * Repository for accessing the outbox table, used for verifying transactional outbox patterns.
     */
    @Autowired
    protected OutboxRepository outboxRepository;

    @Value("${app.kafka.topic.name}")
    protected String topicName;

    /**
     * Cleans up H2 database files after all tests in the class have run.
     */
    @AfterAll
    static void cleanup() {
        try {
            Files.deleteIfExists(Path.of("./data/weatherdb.mv.db"));
            Files.deleteIfExists(Path.of("./data/weatherdb.trace.db"));
        } catch(IOException e) {
            log.warn("Failed to clean up H2 database files", e);
        }
    }

    /**
     * Generates a station ID based on a batch identifier.
     *
     * @param batchId The unique identifier for the batch.
     * @return A formatted station ID string.
     */
    @NotNull
    public static String getStationId(String batchId) {
        return "station-"+batchId;
    }

    /**
     * Provides the Kafka bootstrap servers URL.
     * <p>
     * Subclasses must implement this method to return the appropriate bootstrap server address,
     * which might be from a Testcontainers-managed Kafka instance or an embedded broker.
     * </p>
     *
     * @return The Kafka bootstrap servers URL as a string.
     */
    protected abstract String getBootstrapServers();

    /**
     * Provides the Schema Registry URL.
     * <p>
     * Subclasses must implement this method to return the URL of the Schema Registry.
     * </p>
     *
     * @return The Schema Registry URL as a string.
     * @throws InterruptedException if the thread is interrupted while waiting for the URL.
     */
    protected abstract String getSchemaRegistryUrl() throws InterruptedException;

    /**
     * Sets up the test environment before each test.
     * This method waits for the Kafka broker to be ready, cleans up and recreates the test topic,
     * initializes the Kafka consumer, and clears the outbox repository.
     *
     * @throws Exception if any part of the setup fails.
     */
    @BeforeEach
    void setUp() throws Exception {
        String bootstrap = getBootstrapServers();
        Map<String, Object> adminProps = new HashMap<>(getSecurityProps());
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try(AdminClient admin = AdminClient.create(adminProps)) {
            await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> admin.listTopics().names().get(5, TimeUnit.SECONDS));
            cleanupAndCreateTopic(admin);
        }
        setupConsumer();
        outboxRepository.deleteAll();
    }

    /**
     * Initializes or resets the Kafka consumer for the test.
     * If a consumer instance already exists, it is closed before a new one is created.
     * The new consumer is subscribed to the test topic.
     */
    private void setupConsumer() throws InterruptedException {
        if(consumer!=null) {
            try {
                consumer.close();
            } catch(Exception ignored) {
            }
        }
        consumer = createConsumer(null);
        consumer.subscribe(Collections.singleton(topicName));
        consumer.poll(Duration.ofMillis(500));
        consumer.commitSync();
    }

    /**
     * Sends a weather packet to the ingestion endpoint.
     *
     * @param batchId A unique identifier for the batch of data.
     * @param index   A sequential index for the message within the batch.
     * @throws Exception if the HTTP request fails.
     */
    public void sendWeatherPacket(String batchId, long index) throws Exception {
        long start = System.currentTimeMillis();
        WeatherPacket packet = WeatherPacket.newBuilder()
                .setStationId(getStationId(batchId))
                .setTimestamp(index)
                .build();
        long buildTime = System.currentTimeMillis()-start;

        long beforeSend = System.currentTimeMillis();
        mockMvc.perform(post(API_V_1_WEATHER)
                        .contentType(MediaType.APPLICATION_PROTOBUF)
                        .content(packet.toByteArray()))
                .andExpect(status().is2xxSuccessful());
        long sendTime = System.currentTimeMillis()-beforeSend;

        log.info("Message {}: build={}ms, send={}ms", index, buildTime, sendTime);
    }

    /**
     * Deletes and recreates the Kafka topic to ensure a clean state for the test.
     *
     * @param admin The {@link AdminClient} to use for topic management.
     * @throws Exception if topic deletion or creation fails.
     */
    private void cleanupAndCreateTopic(AdminClient admin) throws Exception {
        var topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
        if(topics.contains(topicName)) {
            admin.deleteTopics(Collections.singletonList(topicName)).all().get(10, TimeUnit.SECONDS);
            Thread.sleep(2000);
        }
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
        admin.createTopics(Collections.singletonList(newTopic)).all().get(10, TimeUnit.SECONDS);
    }

    /**
     * Creates a Kafka consumer with a specific isolation level.
     *
     * @param isolationLevel The transaction isolation level (e.g., "read_committed").
     * @return A new {@link Consumer} instance.
     */
    protected Consumer<String, WeatherPacket> createConsumer(String isolationLevel) throws InterruptedException {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-"+UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        consumerProps.put("schema.registry.url", getSchemaRegistryUrl());
        consumerProps.putAll(getSecurityProps());
        consumerProps.put("specific.protobuf.value.type", WeatherPacket.class.getName());
        if(isolationLevel!=null && !isolationLevel.isBlank()) {
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        }
        return new DefaultKafkaConsumerFactory<String, WeatherPacket>(consumerProps).createConsumer();
    }

    /**
     * Provides security-related properties for Kafka clients.
     * <p>
     * Subclasses can override this method to provide authentication and authorization properties,
     * such as SASL configuration. By default, it returns an empty map, indicating no security.
     * </p>
     *
     * @return A map of security properties for Kafka clients.
     */
    protected Map<String, Object> getSecurityProps() {
        return Collections.emptyMap();
    }

    /**
     * Closes the Kafka consumer after each test to release resources.
     */
    @AfterEach
    void tearDown() {
        if(consumer!=null) {
            try {
                consumer.close();
            } catch(Exception ignored) {
            }
        }
    }
}
