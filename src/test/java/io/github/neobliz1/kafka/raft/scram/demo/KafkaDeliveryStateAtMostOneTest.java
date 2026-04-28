package io.github.neobliz1.kafka.raft.scram.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.neobliz1.kafka.raft.scram.demo.base.BaseToxyProxyTestCase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Slf4j
@ActiveProfiles({ "test-transactions-off", "test" })
@SpringBootTest(properties = {
        "spring.kafka.producer.acks=all",
        "spring.kafka.producer.retries=0",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.delivery.timeout.ms=5000",
        "spring.kafka.producer.properties.max.block.ms=3000",
        "spring.kafka.producer.properties.request.timeout.ms=2000"

})
class KafkaDeliveryStateAtMostOneTest extends BaseToxyProxyTestCase {

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
                Thread.sleep(cutDelay);
                log.info(">>> CUTTING CONNECTION at {} ms <<<", cutDelay);
                KAFKA_PROXY.toxics().timeout("cut", ToxicDirection.UPSTREAM, 0).setTimeout(0);

                Thread.sleep(cutDuration);

                log.info(">>> RESTORING CONNECTION <<<");
                KAFKA_PROXY.toxics().get("cut").remove();
            } catch(Exception e) {
                log.error("Chaos failed", e);
            }
        });
        Thread.sleep(50);
        log.info("Sending {} messages with delays...", totalMessages);

        for(int i = 1; i<=totalMessages; i++) {
            try {
                sendWeatherPacket(testBatchId, i);
                log.info("Message {} sent", i);
            } catch(Exception e) {
                log.info("Message {} failed to send: {}", i, e.getMessage());
            }

            if(i<totalMessages) {
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 300));
            }
        }

        cutFuture.join();
        Thread.sleep(3000);
        await().atMost(15, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                        if(r.value().getStationId().contains(testBatchId)) {
                            received.add((int) r.value().getTimestamp());
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
                            .isLessThan(totalMessages);  // At least one message lost

                    assertThat(received.size())
                            .as("Should have at least some successful messages")
                            .isGreaterThan(0);
                });
    }
}