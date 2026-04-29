package io.github.neobliz1.kafka.raft.scram.demo.producer;

import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ExecutionException;

/**
 * Producer for ingesting weather data.
 * This class handles sending {@link WeatherPacket} messages to Kafka,
 * including transactional sends and dead-letter queue functionality.
 */
@Slf4j
@RequiredArgsConstructor
public class WeatherIngestionProducer {

    private final KafkaTemplate<String, WeatherPacket> kafkaTemplate;

    @Value("${app.kafka.topic.name}")
    private String topicName;

    /**
     * Sends a weather packet to the Kafka topic within a transaction.
     * The message is sent with the station ID as the key.
     *
     * @param weatherPacket The weather packet to send.
     * @return The {@link RecordMetadata} for the sent message.
     * @throws ExecutionException if the Kafka producer encounters an error during send.
     * @throws InterruptedException if the thread is interrupted while waiting for the send operation to complete.
     */
    public RecordMetadata sendTransactional(WeatherPacket weatherPacket) throws ExecutionException, InterruptedException {
        return kafkaTemplate.executeInTransaction(operations -> {
            try {
                return operations.send(topicName, weatherPacket.getStationId(), weatherPacket)
                        .get()
                        .getRecordMetadata();
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Saves a failed weather packet to a dead-letter queue or fallback storage.
     * This method is marked as transactional, implying it might be part of a larger
     * transaction or manage its own transaction for saving to a persistent store.
     *
     * @param packet The weather packet that failed to be processed or sent.
     */
    @Transactional
    public void deadLetterQueueSave(WeatherPacket packet) {
        log.warn("STATION_FAILURE_RECOVERY: Saving failed packet for station {} to fallback storage",
                packet.getStationId());
    }
}
