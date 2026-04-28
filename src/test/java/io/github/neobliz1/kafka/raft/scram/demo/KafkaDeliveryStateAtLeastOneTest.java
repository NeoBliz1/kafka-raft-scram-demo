package io.github.neobliz1.kafka.raft.scram.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.neobliz1.kafka.raft.scram.demo.base.BaseToxyProxyTestCase;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

@Slf4j
@ActiveProfiles({ "test-transactions-off", "test" })
@SpringBootTest(properties = {
        "spring.kafka.producer.acks=all",
        "spring.kafka.producer.retries=10",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.request.timeout.ms=3000",
        "spring.kafka.producer.properties.delivery.timeout.ms=30000",
        "spring.kafka.producer.properties.retry.backoff.ms=500",
})
class KafkaDeliveryStateAtLeastOneTest extends BaseToxyProxyTestCase {

    @Test
    void verifyAtLeastOnceDeliveryWithLatencyToxic() throws Exception {
        String warmupStationId = "warmup-"+UUID.randomUUID();
        String testStationId = "delivery-test-"+UUID.randomUUID();
        List<WeatherPacket> allReceived = new CopyOnWriteArrayList<>();

        // STEP 1: Establish connection with warmup message
        log.info("STEP 1: Sending warmup message to establish connection...");
        sendWeatherPacket(warmupStationId, Instant.now().toEpochMilli());

        Thread.sleep(2000);

        // STEP 2: Add LATENCY toxic with delay > request.timeout.ms
        // request.timeout.ms = 5000, so use 8000ms delay
        log.info("STEP 2: Adding LATENCY toxic (8 second delay)...");
        KAFKA_PROXY.toxics()
                .latency("kafka_latency", ToxicDirection.UPSTREAM, 8000)
                .setJitter(1000);

        Thread.sleep(1000);

        // STEP 3: Send test message - should timeout and retry
        log.info("STEP 3: Sending test message with delayed ACK...");
        CompletableFuture<Void> sendFuture = CompletableFuture.runAsync(() -> {
            try {
                sendWeatherPacket(testStationId, Instant.now().toEpochMilli());

                log.info("Test message send completed");
            } catch(Exception e) {
                log.error("Send failed", e);
                throw new RuntimeException(e);
            }
        });

        // Wait longer for retries (10 retries * 1s backoff = ~10s, plus delays)
        log.info("Waiting for producer retries...");
        Thread.sleep(25000);

        // STEP 4: Remove latency toxic
        log.info("STEP 4: Removing latency toxic...");
        KAFKA_PROXY.toxics().get("kafka_latency").remove();

        Thread.sleep(5000);
        sendFuture.get(30, TimeUnit.SECONDS);

        // STEP 5: Verify duplicates
        await().atMost(90, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            // Poll multiple times
            for(int i = 0; i<20; i++) {
                ConsumerRecords<String, WeatherPacket> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(record -> {
                    if(record.value().getStationId().equals(getStationId(testStationId))) {
                        allReceived.add(record.value());
                        log.info("Received test message at offset: {}, timestamp: {}",
                                record.offset(), record.value().getTimestamp());
                    }
                });
                Thread.sleep(500);
            }

            log.info("Total test messages received: {}", allReceived.size());

            // Verify we have duplicates
            assertThat(allReceived.size())
                    .withFailMessage("Expected duplicates (2+ messages) but got %d", allReceived.size())
                    .isGreaterThan(1);

            // Verify all messages are identical (proves it's retry-induced duplicates)
            WeatherPacket first = allReceived.getFirst();
            assertThat(allReceived)
                    .withFailMessage("Not all messages are identical - they should be duplicates from producer retries")
                    .allMatch(msg -> msg.getStationId().equals(first.getStationId()) &&
                            msg.getTimestamp()==first.getTimestamp());

            log.info("✅ SUCCESS: Detected {} IDENTICAL messages, proving at-least-once delivery with duplicates!",
                    allReceived.size());
        });
    }
}
