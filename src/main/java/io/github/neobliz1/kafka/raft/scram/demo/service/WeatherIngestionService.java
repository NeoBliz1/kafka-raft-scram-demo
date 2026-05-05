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
 * <p>
 * This service contains the core business logic for processing weather packets.
 * It demonstrates a transactional outbox pattern and includes methods to simulate
 * system failures to test for message delivery guarantees, such as exactly-once semantics.
 * </p>
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
     * Asynchronously sends a weather packet to Kafka.
     * <p>
     * This method uses a {@link CompletableFuture} to send the packet without blocking.
     * It wraps the call to the transactional producer. On successful send, it logs the
     * metadata. On failure, it logs the error and forwards the packet to a dead-letter queue.
     * </p>
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
     * Processes a weather packet with a simulated failure to test transactional integrity.
     * <p>
     * This method implements a partial transactional outbox pattern to demonstrate a
     * potential failure mode. It performs the following steps:
     * 1. Saves the message to a database outbox table with a 'PENDING' status.
     * 2. Sends the message to Kafka within a separate transaction.
     * 3. If {@code forceFailure} is true, it throws a {@link RuntimeException} to simulate
     *    a crash *after* the Kafka message has been committed but *before* the database
     *    transaction for the outbox status update can commit.
     * </p>
     * This leaves the system in a state where the message is in Kafka, but the outbox
     * record still says 'PENDING', leading to a duplicate send upon recovery.
     *
     * @param packet       The weather packet to process.
     * @param forceFailure If true, a runtime exception is thrown to simulate a crash.
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
     * Simulates a manual recovery process for pending outbox messages.
     * <p>
     * This method is designed to be called after a simulated failure. It does the following:
     * 1. Resets the Kafka producer factory to simulate an application restart, which ensures
     *    a new Producer ID (PID) is assigned.
     * 2. Finds all outbox records with a 'PENDING' status.
     * 3. For each pending record, it re-sends the message to Kafka in a new transaction.
     * 4. Updates the outbox record status to 'PROCESSED'.
     * </p>
     * This process demonstrates how a naive recovery can lead to duplicate messages in Kafka.
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
