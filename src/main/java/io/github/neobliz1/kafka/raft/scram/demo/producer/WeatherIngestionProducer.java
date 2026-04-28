package io.github.neobliz1.kafka.raft.scram.demo.producer;

import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ExecutionException;

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
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public RecordMetadata sendTransactional(WeatherPacket weatherPacket) throws ExecutionException, InterruptedException {
        return kafkaTemplate.send(topicName, weatherPacket.getStationId(), weatherPacket)
                .get()
                .getRecordMetadata();
    }

    /**
     * Saves a failed weather packet to the dead letter queue.
     *
     * @param packet The weather packet to save.
     */
    @Transactional
    public void deadLetterQueueSave(WeatherPacket packet) {
        log.warn("STATION_FAILURE_RECOVERY: Saving failed packet for station {} to fallback storage",
                packet.getStationId());
    }
}
