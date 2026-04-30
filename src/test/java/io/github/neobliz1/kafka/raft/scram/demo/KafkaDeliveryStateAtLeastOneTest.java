package io.github.neobliz1.kafka.raft.scram.demo;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.neobliz1.kafka.raft.scram.demo.base.BaseToxyProxyTestCase;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Integration test for "at-least-once" Kafka delivery semantics.
 * This test uses ToxiProxy to introduce network latency, simulating conditions
 * where producers might retry sending messages, leading to duplicates.
 * It verifies that messages are delivered at least once, potentially with duplicates.
 */
@Slf4j
@ActiveProfiles({ "test-transactions-off", "test" })
@SpringBootTest(properties = {
        "spring.kafka.producer.acks=all",
        "spring.kafka.producer.retries=5",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.request.timeout.ms=2000",
        "spring.kafka.producer.properties.delivery.timeout.ms=15000",
        "spring.kafka.producer.properties.retry.backoff.ms=300",
})
class KafkaDeliveryStateAtLeastOneTest extends BaseToxyProxyTestCase {

    @Container
    static ComposeContainer ENVIRONMENT = new ComposeContainer(new File("kafka/docker-compose-toxy-proxy.yml"))
            .withExposedService(TOXIPROXY, TOXI_PORT_1, Wait.forListeningPort())
            .withExposedService(KAFKA, KAFKA_PORT, Wait.forListeningPort())
            .withExposedService(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT, Wait.forHttp("/subjects")
                    .forPort(SCHEMA_REGISTRY_PORT).forStatusCode(200))
            .withExposedService(TOXIPROXY, TOXI_PORT_2, Wait.forListeningPort());

    @BeforeAll
    static void setupContainers() {
        setupToxiproxy(ENVIRONMENT);
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> PROXY_BOOTSTRAP_ADDRESS);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                () -> "http://localhost:"+ENVIRONMENT.getServicePort(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT));
        registry.add("spring.kafka.admin.properties.bootstrap.servers", () -> PROXY_BOOTSTRAP_ADDRESS);
    }

    @Override
    protected String getSchemaRegistryUrl() {
        return "http://localhost:"+ENVIRONMENT.getServicePort(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT);
    }

    @Test
    void verifyAtLeastOnceDeliveryWithLatencyToxic() throws Exception {
        String warmupStationId = "warmup-"+UUID.randomUUID();
        String testStationId = "delivery-test-"+UUID.randomUUID();
        List<WeatherPacket> allReceived = new CopyOnWriteArrayList<>();
        long startTime = System.currentTimeMillis();

        log.info("STEP 1: Quick warmup...");
        sendWeatherPacket(warmupStationId, Instant.now().toEpochMilli());

        await().atMost(2, SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, WeatherPacket> records = consumer.poll(Duration.ofMillis(300));
            assertThat(records).isNotEmpty();
        });

        int latencyMs = 2500;
        log.info("STEP 2: Adding {}ms latency toxic...", latencyMs);
        KAFKA_PROXY.toxics()
                .latency("kafka_latency", ToxicDirection.UPSTREAM, latencyMs)
                .setJitter(300);

        await().pollDelay(300, MILLISECONDS).until(() -> true);

        log.info("STEP 3: Sending test message...");
        CompletableFuture<Void> sendFuture = CompletableFuture.runAsync(() -> {
            try {
                sendWeatherPacket(testStationId, Instant.now().toEpochMilli());
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        });

        await().pollDelay(4500, MILLISECONDS).until(() -> true);

        log.info("STEP 4: Removing latency...");
        KAFKA_PROXY.toxics().get("kafka_latency").remove();

        await().pollDelay(1, SECONDS).until(() -> true);
        sendFuture.get(5, SECONDS);

        await().atMost(5, SECONDS)
                .pollInterval(300, MILLISECONDS)
                .untilAsserted(() -> {
                    ConsumerRecords<String, WeatherPacket> records = consumer.poll(Duration.ofMillis(500));
                    records.forEach(record -> {
                        if(record.value().getStationId().equals(getStationId(testStationId))) {
                            allReceived.add(record.value());
                        }
                    });

                    await().pollDelay(500, MILLISECONDS).until(() -> true);

                    ConsumerRecords<String, WeatherPacket> secondBatch = consumer.poll(Duration.ofMillis(500));
                    secondBatch.forEach(record -> {
                        if(record.value().getStationId().equals(getStationId(testStationId))) {
                            allReceived.add(record.value());
                        }
                    });

                    log.info("Received {} messages", allReceived.size());

                    assertThat(allReceived.size())
                            .withFailMessage("Expected duplicates but got %d", allReceived.size())
                            .isGreaterThan(1);

                    WeatherPacket first = allReceived.getFirst();
                    assertThat(allReceived).allMatch(msg ->
                            msg.getStationId().equals(first.getStationId()) &&
                                    msg.getTimestamp()==first.getTimestamp());

                    log.info("✅ SUCCESS in ~{} seconds!",
                            MILLISECONDS.toSeconds(System.currentTimeMillis()-startTime));
                });
    }
}
