package io.github.neobliz1.kafka.raft.scram.demo;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.github.neobliz1.kafka.raft.scram.demo.base.BaseKafkaTestCase;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Integration test for weather data ingestion using a Kafka cluster with SCRAM-SHA-512 authentication.
 * <p>
 * This test leverages Testcontainers and Docker Compose to spin up a complete Kafka environment,
 * including a broker and Schema Registry, configured with SCRAM-SHA-512 for secure communication.
 * It validates that the application can correctly produce and consume messages in this
 * authenticated and encrypted setup.
 * </p>
 */
@Slf4j
@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test-scram-auth")
class WeatherIngestionControllerTestContainerWithScramAuthTest extends BaseKafkaTestCase {

    protected static String BOOTSTRAP_SERVERS_URL;
    protected static String REGISTRY_URL;

    /**
     * The Docker Compose container environment for the Kafka cluster.
     * This sets up Kafka and Schema Registry with SCRAM-SHA-512 authentication.
     */
    @Container
    static ComposeContainer ENVIRONMENT = new ComposeContainer(new File("kafka/docker-compose-scram-auth-kafka.yml"))
            .withEnv("KAFKA_ADMIN", "admin")
            .withEnv("KAFKA_ADMIN_PASSWORD", "admin-password")
            .withEnv("KAFKA_USER", "client")
            .withEnv("KAFKA_PASSWORD", "client-secret")
            .withExposedService("kafka", 9092)
            .withExposedService("schema-registry", 8081, Wait.forHttp("/subjects")
                    .forPort(8081)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(2)))
            .withLogConsumer("kafka", new Slf4jLogConsumer(log).withPrefix("[KAFKA]"))
            .withLogConsumer("schema-registry", new Slf4jLogConsumer(log).withPrefix("[REGISTRY]"));

    @Autowired
    private KafkaProperties kafkaProperties;

    /**
     * Overrides Spring Boot properties at runtime to use the dynamic URLs from the Testcontainers.
     *
     * @param registry The dynamic property registry.
     */
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        BOOTSTRAP_SERVERS_URL = ENVIRONMENT.getServiceHost("kafka", 9092)
                +":"+ENVIRONMENT.getServicePort("kafka", 9092);
        REGISTRY_URL = "http://"+ENVIRONMENT.getServiceHost("schema-registry", 8081)
                +":"+ENVIRONMENT.getServicePort("schema-registry", 8081);

        registry.add("spring.kafka.bootstrap-servers", () -> BOOTSTRAP_SERVERS_URL);
        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> REGISTRY_URL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getBootstrapServers() {
        return BOOTSTRAP_SERVERS_URL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getSchemaRegistryUrl() {
        return REGISTRY_URL;
    }

    /**
     * Provides the SCRAM-SHA-512 security properties for the Kafka client.
     * These properties are derived from the Spring Kafka configuration.
     *
     * @return A map of security properties.
     */
    @Override
    protected Map<String, Object> getSecurityProps() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        log.info("Security props: ");
        for(Map.Entry<String, Object> prop : props.entrySet()) {
            log.info("{}: {}", prop.getKey(), prop.getValue());
        }
        return props;
    }

    /**
     * Tests the end-to-end flow of ingesting a weather data packet and verifying
     * its consumption from the Kafka topic in a SCRAM-SHA-512 secured environment.
     *
     * @throws Exception if the test fails.
     */
    @Test
    void shouldIngestAndProduceWeatherDataWithScram() throws Exception {
        String batchId = UUID.randomUUID().toString();

        sendWeatherPacket(batchId, Instant.now().toEpochMilli());

        await().atMost(20, SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    ConsumerRecords<String, WeatherPacket> records = consumer.poll(Duration.ofMillis(200));
                    assertThat(records).withFailMessage("No records received. Check Kafka logs for Auth errors.").isNotEmpty();

                    WeatherPacket consumedPacket = records.iterator().next().value();
                    assertThat(consumedPacket.getStationId()).isEqualTo(getStationId(batchId));
                    log.info("Successfully consumed packet with stationId: {}", consumedPacket.getStationId());
                });
    }
}
