package io.github.neobliz1.kafka.raft.scram.demo.service;


import io.github.neobliz1.kafka.raft.scram.demo.domain.Outbox;
import io.github.neobliz1.kafka.raft.scram.demo.producer.WeatherIngestionProducer;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import io.github.neobliz1.kafka.raft.scram.demo.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CompletableFuture;

/**
 * Service for ingesting weather data.
 * Handles the business logic for processing and sending weather packets to Kafka,
 * including transactional outbox patterns and simulated failure scenarios.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WeatherIngestionService {

    private final WeatherIngestionProducer weatherIngestionProducer;
    private final KafkaTemplate<String, WeatherPacket> kafka;
    private final TransactionTemplate transactionTemplate;
    private final OutboxRepository repo;

    @Value("${app.kafka.topic.name}")
    private String topicName;


    /**
     * Sends a weather packet to the ingestion producer.
     * This method uses a CompletableFuture to send the packet asynchronously
     * and handles success or failure by logging and potentially saving to a dead-letter queue.
     *
     * @param weatherPacket The weather packet to send.
     */
    public void sendWeatherPacket(WeatherPacket weatherPacket) {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return weatherIngestionProducer.sendTransactional(weatherPacket);
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
        ).whenComplete((metadata, ex) -> {
            if(ex==null) log.info("Offset: {}", metadata.offset());
            else {
                log.error("Failed to ingest station {}: {}", weatherPacket.getStationId(), ex.getCause().getMessage());
                weatherIngestionProducer.deadLetterQueueSave(weatherPacket);
            }
        });
    }

    /**
     * Processes a weather packet with a simulated failure mechanism.
     * This method demonstrates the transactional outbox pattern where a database
     * operation and a Kafka send operation are attempted. A forced failure
     * can simulate a crash between the Kafka commit and the database update.
     *
     * @param packet       The weather packet to process.
     * @param forceFailure If true, a runtime exception is thrown after Kafka commit to simulate a database crash.
     */
    public void processWithSimulatedFailure(WeatherPacket packet, boolean forceFailure) {
        // 1. Save to H2 (This will rollback if forceFailure is true)
        transactionTemplate.execute(status -> {
            Outbox entity = new Outbox();
            entity.setPayload(packet.toByteArray());
            entity.setStationId(packet.getStationId());
            entity.setStatus("PENDING");
            return repo.save(entity);
        });

        // 2. FORCE KAFKA COMMIT
        kafka.executeInTransaction(t -> {
            t.send(topicName, packet.getStationId(), packet);
            return null;
        });

        kafka.flush();

        // 3. THE CRASH
        if(forceFailure) {
            log.warn("SIMULATING DB CRASH for station: {}", packet.getStationId());
            // This triggers a ROLLBACK for JPA (Entity stays PENDING or is removed)
            // But the Kafka message from step 2 is already COMMITTED.
            throw new RuntimeException("DB Update Failed");
        }

        // 4. Update status (Only reached if no failure)
        transactionTemplate.execute(status -> {
            Outbox entity = repo.findByStationIdAndStatus(packet.getStationId(), "PENDING");
            entity.setStatus("PROCESSED");
            return repo.save(entity);
        });
    }

    /**
     * Recovers pending messages from the outbox.
     * This method simulates a recovery process after a potential failure,
     * where messages that were committed to Kafka but not updated in the database
     * are re-sent and their status updated.
     */
    public void manualRecovery() {
        // 1. Force a new Producer ID (Simulates app restart)
        kafka.getProducerFactory().reset();

        repo.findByStatus("PENDING").forEach(entity -> {
            try {
                WeatherPacket p = WeatherPacket.parseFrom(entity.getPayload());

                // 2. Explicitly wrap the recovery send in a transaction
                kafka.executeInTransaction(operations -> {
                    operations.send(topicName, entity.getStationId(), p);
                    return null;
                });

                // 3. Update DB status after Kafka acknowledges
                entity.setStatus("PROCESSED");
                repo.save(entity);

            } catch(Exception e) {
                log.error("Recovery failed for station: {}", entity.getStationId(), e);
            }
        });
    }
}
