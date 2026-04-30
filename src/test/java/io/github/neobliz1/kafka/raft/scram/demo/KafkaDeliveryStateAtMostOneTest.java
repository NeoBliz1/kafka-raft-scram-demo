package io.github.neobliz1.kafka.raft.scram.demo;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.neobliz1.kafka.raft.scram.demo.base.BaseToxyProxyTestCase;
import lombok.extern.slf4j.Slf4j;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Integration test for "at-most-once" Kafka delivery semantics.
 * This test simulates network disruptions using ToxiProxy to verify that
 * messages are not duplicated, even if some might be lost during network cuts.
 */
@Slf4j
@ActiveProfiles({ "test-transactions-off", "test" })
@SpringBootTest(properties = {
        "spring.kafka.producer.acks=all",
        "spring.kafka.producer.retries=0",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.delivery.timeout.ms=5000",
        "spring.kafka.producer.properties.max.block.ms=5000",
        "spring.kafka.producer.properties.request.timeout.ms=2000"

})
class KafkaDeliveryStateAtMostOneTest extends BaseToxyProxyTestCase {

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

    /**
     * Verifies partial message loss and no duplicates when a random connection cut occurs.
     * This test simulates a network outage during message production and asserts that
     * while some messages may be lost (at-most-once), no messages are duplicated.
     *
     * @throws Exception if any error occurs during the test execution.
     */
    @Test
    void verifyPartialLossOnRandomConnectionCut() throws Exception {
        log.info("WARMUP: Initializing producer...");
        sendWeatherPacket("warmup-"+UUID.randomUUID(), 0);
        log.info("WARMUP complete");

        String testBatchId = UUID.randomUUID().toString();
        int totalMessages = 10;
        int cutDelay = 500;
        int cutDuration = 2000;
        List<Integer> received = new CopyOnWriteArrayList<>();

        CompletableFuture<Void> cutFuture = CompletableFuture.runAsync(() -> {
            try {
                await().pollDelay(cutDelay, MILLISECONDS).until(() -> true);
                log.info(">>> CUTTING CONNECTION at {} ms <<<", cutDelay);
                KAFKA_PROXY.toxics().timeout("cut", ToxicDirection.UPSTREAM, 0).setTimeout(0);

                await().pollDelay(cutDuration, MILLISECONDS).until(() -> true);

                log.info(">>> RESTORING CONNECTION <<<");
                KAFKA_PROXY.toxics().get("cut").remove();
            } catch(Exception e) {
                log.error("Chaos failed", e);
            }
        });

        await().pollDelay(50, MILLISECONDS).until(() -> true);
        log.info("Sending {} messages with delays...", totalMessages);

        for(int i = 1; i<=totalMessages; i++) {
            try {
                sendWeatherPacket(testBatchId, i);
                log.info("Message {} sent", i);
            } catch(Exception e) {
                log.info("Message {} failed to send: {}", i, e.getMessage());
            }

            if(i<totalMessages) {
                int randomDelay = ThreadLocalRandom.current().nextInt(100, 300);
                await().pollDelay(randomDelay, MILLISECONDS).until(() -> true);
            }
        }

        cutFuture.join();
        await().pollDelay(3, SECONDS).until(() -> true);

        await().atMost(15, SECONDS)
                .pollInterval(500, MILLISECONDS)
                .untilAsserted(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                        if(r.value().getStationId().contains(testBatchId)) {
                            int timestamp = (int) r.value().getTimestamp();
                            if(!received.contains(timestamp)) {
                                received.add(timestamp);
                            }
                        }
                    });

                    log.info("Status: {} of {} messages delivered", received.size(), totalMessages);
                    log.info("Delivered timestamps: {}", received);
                    log.info("Cut window: {}-{}ms", cutDelay, cutDelay+cutDuration);

                    assertThat(received)
                            .as("At-most-once check: No duplicates allowed")
                            .doesNotHaveDuplicates();

                    assertThat(received.size())
                            .as("Partial loss check: Expected some message loss due to network cut")
                            .isLessThan(totalMessages);

                    assertThat(received.size())
                            .as("Should have at least some successful messages")
                            .isGreaterThan(0);
                });
    }
}