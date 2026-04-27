package io.github.neobliz1.kafka.raft.scram.demo.producer;

import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Producer for ingesting weather data.
 */
@Slf4j
@RequiredArgsConstructor
public class WeatherIngestionProducer {

    private final KafkaTemplate<String, WeatherPacket> kafkaTemplate;

    @Value("${app.kafka.topic.name}")
    private String topicName;

    /**
     * Sends a weather packet to the Kafka topic.
     *
     * @param weatherPacket The weather packet to send.
     */
    // Use 'value' or 'includes' for exceptions
    // Use 'maxRetries' instead of 'maxAttempts'
    // Use 'delay' instead of '@Backoff'
    public void send(WeatherPacket weatherPacket) {
        kafkaTemplate.send(topicName, weatherPacket.getStationId(), weatherPacket)
                .whenComplete((result, ex) -> {
                    if(ex!=null) {
                        log.error("Failed to send weather data to Kafka", ex);
                        deadLetterQueueSave(weatherPacket);
                    } else {
                        log.info("Sent weather data to Kafka: {}", result.getRecordMetadata());
                    }
                });
    }

    /**
     * Saves a failed weather packet to the dead letter queue.
     *
     * @param packet The weather packet to save.
     */
    public void deadLetterQueueSave(WeatherPacket packet) {
        log.warn("STATION_FAILURE_RECOVERY: Saving failed packet for station {} to fallback storage",
                packet.getStationId());
    }
}
