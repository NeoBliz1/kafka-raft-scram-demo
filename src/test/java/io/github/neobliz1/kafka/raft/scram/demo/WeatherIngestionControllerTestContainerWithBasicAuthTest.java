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

@Slf4j
@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test-basic-auth")
class WeatherIngestionControllerTestContainerWithBasicAuthTest extends BaseKafkaTestCase {

    protected static String BOOTSTRAP_SERVERS_URL;
    protected static String REGISTRY_URL;

    @Container
    static ComposeContainer ENVIRONMENT = new ComposeContainer(new File("kafka/docker-compose-basic-auth-kafka.yml"))
            // Match the credentials defined in your PLAIN JAAS config
            .withEnv("KAFKA_USER", "client")
            .withEnv("KAFKA_PASSWORD", "client-secret")
            .withExposedService(KAFKA, KAFKA_PORT)
            .withExposedService(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT, Wait.forHttp("/subjects")
                    .forPort(SCHEMA_REGISTRY_PORT)
                    .forStatusCode(200))
            .withLogConsumer(KAFKA, new Slf4jLogConsumer(log).withPrefix("[KAFKA]"))
            .withLogConsumer(SCHEMA_REGISTRY, new Slf4jLogConsumer(log).withPrefix("[REGISTRY]"))
            .withLogConsumer("kafka-setup", new Slf4jLogConsumer(log).withPrefix("[SETUP]"));
    @Autowired
    private KafkaProperties kafkaProperties;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        BOOTSTRAP_SERVERS_URL = ENVIRONMENT.getServiceHost(KAFKA, KAFKA_PORT)+":"+ENVIRONMENT.getServicePort(KAFKA, KAFKA_PORT);
        REGISTRY_URL = "http://"+ENVIRONMENT.getServiceHost(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT)
                +":"+ENVIRONMENT.getServicePort(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT);

        registry.add("spring.kafka.bootstrap-servers", () -> BOOTSTRAP_SERVERS_URL);
        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> REGISTRY_URL);
    }

    @Override
    protected String getBootstrapServers() {
        return BOOTSTRAP_SERVERS_URL;
    }

    @Override
    protected String getSchemaRegistryUrl() {
        return REGISTRY_URL;
    }

    @Override
    protected Map<String, Object> getSecurityProps() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        log.info("Security props: ");
        for(Map.Entry<String, Object> prop : props.entrySet()) {
            log.info("{}: {}", prop.getKey(), prop.getValue());
        }
        return props;
    }

    @Test
    void shouldIngestAndProduceWeatherData() throws Exception {
        String batchId = UUID.randomUUID().toString();

        sendWeatherPacket(batchId, Instant.now().toEpochMilli());

        await().atMost(15, SECONDS)
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    ConsumerRecords<String, WeatherPacket> records = consumer.poll(Duration.ofMillis(100));
                    assertThat(records).withFailMessage("No records received from Kafka").isNotEmpty();

                    WeatherPacket consumedPacket = records.iterator().next().value();
                    assertThat(consumedPacket.getStationId()).isEqualTo(getStationId(batchId));
                });
    }
}
