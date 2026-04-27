package io.github.neobliz1.kafka.raft.scram.demo.producer;

import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.resilience.annotation.Retryable;

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
    @Retryable(
            value = { Exception.class },
            maxRetries = 3,
            delay = 2000
    )
    public void send(WeatherPacket weatherPacket) {
        kafkaTemplate.send(topicName, weatherPacket.getStationId(), weatherPacket)
                .whenComplete((result, ex) -> {
                    if(ex!=null) {
                        log.error("Failed to send weather data to Kafka", ex);
                    } else {
                        log.info("Sent weather data to Kafka: {}", result.getRecordMetadata());
                    }
                });
    }
}
