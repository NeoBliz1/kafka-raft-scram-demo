package io.github.neobliz1.kafka.raft.scram.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest(properties = {
        "spring.kafka.producer.acks=all",
        "spring.kafka.producer.retries=0",
        "spring.kafka.producer.properties.enable.idempotence=false",
})
class KafkaDeliveryStateAtMostOneTest extends BaseToxyProxyTestCase {

    @Test
    void verifyPartialLossOnRandomConnectionCut() throws Exception {
        log.info("WARMUP: Initializing producer...");
        sendWeatherPacket("warmup-"+UUID.randomUUID(), 0);
        log.info("WARMUP complete");
        String testBatchId = UUID.randomUUID().toString();
        int totalMessages = 4, cutInterval = 100;
        int intervalBeforeCut = ThreadLocalRandom.current().nextInt(0, 201);
        List<Integer> received = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> cutFuture = CompletableFuture.runAsync(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(intervalBeforeCut);
                log.info(">>> CUTTING CONNECTION at {} ms <<<", intervalBeforeCut);
                KAFKA_PROXY.toxics().timeout("cut", ToxicDirection.UPSTREAM, 0).setTimeout(0);

                TimeUnit.MILLISECONDS.sleep(cutInterval);

                log.info(">>> RESTORING CONNECTION <<<");
                KAFKA_PROXY.toxics().get("cut").remove();
            } catch(Exception e) {
                log.error("Chaos failed", e);
            }
        });

        log.info("Sending {} messages...", totalMessages);
        for(int i = 1; i<=totalMessages; i++) {
            sendWeatherPacket(testBatchId, i);
            if(i<totalMessages) Thread.sleep(100);
        }

        cutFuture.join();
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                        if(r.value().getStationId().contains(testBatchId)) {
                            received.add((int) r.value().getTimestamp());
                        }
                    });

                    log.info("Status: Received {} | Cut Window: {}-{}ms",
                            received, intervalBeforeCut, intervalBeforeCut+cutInterval);

                    assertThat(received)
                            .as("At-most-once check: No duplicates allowed")
                            .doesNotHaveDuplicates();

                    assertThat(received.size())
                            .as("Partial loss check: Expected 1-3 messages delivered")
                            .isBetween(1, 3);
                });
    }
}