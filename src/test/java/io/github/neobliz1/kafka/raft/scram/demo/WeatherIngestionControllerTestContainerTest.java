package io.github.neobliz1.kafka.raft.scram.demo;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
class WeatherIngestionControllerTestContainerTest extends BaseKafkaTestCase {

    protected static String BOOTSTRAP_SERVERS_URL;
    protected static String REGISTRY_URL;

    @Container
    static ComposeContainer ENVIRONMENT = new ComposeContainer(new File("kafka/docker-compose-no-auth-kafka.yml"))
            .withExposedService(KAFKA, KAFKA_PORT)
            .withExposedService(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT, Wait.forHttp("/subjects")
                    .forPort(SCHEMA_REGISTRY_PORT)
                    .forStatusCode(200));

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
