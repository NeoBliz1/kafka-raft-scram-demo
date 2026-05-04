package io.github.neobliz1.kafka.raft.scram.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.neobliz1.kafka.raft.scram.demo.base.BaseKafkaTestCase;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import io.github.neobliz1.kafka.raft.scram.demo.service.WeatherIngestionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for demonstrating "exactly-once" delivery challenges and solutions
 * in Kafka when idempotence is enabled but transactions are not fully coordinated
 * across services (e.g., database and Kafka).
 * This test uses Testcontainers to set up a Kafka environment.
 */
@Slf4j
@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "spring.kafka.producer.acks=all",
        "spring.kafka.producer.retries=10",
        "spring.kafka.producer.properties.enable.idempotence=true",
        "spring.kafka.producer.properties.request.timeout.ms=5000",
        "spring.kafka.producer.properties.delivery.timeout.ms=60000",
        "spring.kafka.producer.properties.retry.backoff.ms=1000",
        "spring.kafka.producer.custom-transaction-id-prefix=test-tx-"
})
class KafkaDeliveryStateExactlyOnceWithDuplicatesTest extends BaseKafkaTestCase {

    protected static String BOOTSTRAP_SERVERS_URL;
    protected static String REGISTRY_URL;

    /**
     * The Docker Compose container environment, which includes Kafka and Schema Registry services.
     */
    @Container
    static ComposeContainer ENVIRONMENT = new ComposeContainer(new File("kafka/docker-compose-no-auth-kafka.yml"))
            .withExposedService(KAFKA, KAFKA_PORT)
            .withExposedService(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT, Wait.forHttp("/subjects")
                    .forPort(SCHEMA_REGISTRY_PORT)
                    .forStatusCode(200));
    @Autowired
    private WeatherIngestionService weatherIngestionService;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        BOOTSTRAP_SERVERS_URL = ENVIRONMENT.getServiceHost(KAFKA, KAFKA_PORT)+":"+ENVIRONMENT.getServicePort(KAFKA, KAFKA_PORT);
        REGISTRY_URL = "http://"+ENVIRONMENT.getServiceHost(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT)
                +":"+ENVIRONMENT.getServicePort(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT);

        registry.add("spring.kafka.bootstrap-servers", () -> BOOTSTRAP_SERVERS_URL);
        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> REGISTRY_URL);
        registry.add("spring.kafka.admin.properties.bootstrap.servers", () -> BOOTSTRAP_SERVERS_URL);
    }

    @Override
    protected String getBootstrapServers() {
        return BOOTSTRAP_SERVERS_URL;
    }

    @Override
    protected String getSchemaRegistryUrl() {
        return REGISTRY_URL;
    }

    /**
     * Verifies that the Kafka topic is ready before each test.
     * This ensures that the topic exists and has partitions, allowing producers and consumers to operate.
     */
    @BeforeEach
    void verifyTopicIsReady() {
        String bootstrap = getBootstrapServers();
        try(var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            await().atMost(15, TimeUnit.SECONDS).until(() -> {
                try {
                    var description = admin.describeTopics(Collections.singleton(topicName)).allTopicNames().get();
                    return description.containsKey(topicName) &&
                            !description.get(topicName).partitions().isEmpty();
                } catch(Exception e) {
                    return false;
                }
            });
        }
    }

    /**
     * Tests a scenario where a message is sent to Kafka, but a subsequent database
     * operation fails, leading to a "duplicate" message being sent upon recovery.
     * This highlights the need for proper transactional outbox patterns to achieve
     * exactly-once semantics when coordinating between a database and Kafka.
     * The test verifies that two identical messages (based on business key) are
     * present in Kafka, but at different offsets, demonstrating the duplicate.
     */
    @Test
    void testExactlyOnceDuplicateSimulation() {
        String batchId = UUID.randomUUID().toString();
        WeatherPacket packet = WeatherPacket.newBuilder()
                .setStationId(getStationId(batchId))
                .setTimestamp(System.currentTimeMillis())
                .build();

        // 1. Trigger Failure
        // Kafka message is sent/committed, but DB rolls back to 'PENDING'
        assertThrows(RuntimeException.class, () ->
                weatherIngestionService.processWithSimulatedFailure(packet, true)
        );

        // 2. Run Recovery
        // It finds the 'PENDING' record and sends it AGAIN
        // Since this is a new transaction, it gets a NEW Producer ID (PID)
        weatherIngestionService.manualRecovery();

        // 3. Verify Duplicates in Kafka
        // Use 'read_committed' to show that BOTH are valid, committed messages
        try(Consumer<String, WeatherPacket> testConsumer = createConsumer("read_committed")) {
            testConsumer.subscribe(Collections.singleton(topicName));

            List<ConsumerRecord<String, WeatherPacket>> records = new ArrayList<>();
            await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
                KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(2)).forEach(records::add);

                List<ConsumerRecord<String, WeatherPacket>> filteredRecords = records.stream()
                        .filter(r -> r.key().equals(getStationId(batchId)))
                        .sorted(Comparator.comparingLong(ConsumerRecord::offset))
                        .toList();

                assertThat(filteredRecords)
                        .withFailMessage("Expected 2 records but found "+filteredRecords.size())
                        .hasSize(2);

                WeatherPacket firstMsg = filteredRecords.get(0).value();
                WeatherPacket secondMsg = filteredRecords.get(1).value();

                // 1. Check if Business Keys are identical
                assertThat(firstMsg.getStationId()).isEqualTo(secondMsg.getStationId());
                assertThat(firstMsg.getTimestamp()).isEqualTo(secondMsg.getTimestamp());

                // 2. Double-check offsets are different to prove they are unique log entries
                assertThat(filteredRecords.get(0).offset())
                        .isLessThan(filteredRecords.get(1).offset());

                log.info("Verified Duplicate: Station {} at Timestamp {} exists at two different offsets: {} and {}",
                        firstMsg.getStationId(), firstMsg.getTimestamp(),
                        filteredRecords.get(0).offset(), filteredRecords.get(1).offset());
            });
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }

        log.info("Test Successful: Found 2 committed duplicates for station {}", batchId);
    }
}