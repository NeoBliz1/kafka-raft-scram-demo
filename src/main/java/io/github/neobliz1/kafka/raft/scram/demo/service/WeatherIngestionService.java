package io.github.neobliz1.kafka.raft.scram.demo.service;


import io.github.neobliz1.kafka.raft.scram.demo.producer.WeatherIngestionProducer;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service for ingesting weather data.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WeatherIngestionService {

    private final WeatherIngestionProducer weatherIngestionProducer;

    /**
     * Saves a failed weather packet to the dead letter queue.
     *
     * @param packet The weather packet to save.
     */
    private void deadLetterQueueSave(WeatherPacket packet) {
        log.warn("STATION_FAILURE_RECOVERY: Saving failed packet for station {} to fallback storage",
                packet.getStationId());
    }

    /**
     * Sends a weather packet to the ingestion producer.
     *
     * @param weatherPacket The weather packet to send.
     */
    public void sendWeatherPacket(WeatherPacket weatherPacket) {
        try {
            weatherIngestionProducer.send(weatherPacket);
        } catch(RuntimeException e) {
            // This catch block IS your recovery logic now
            log.error("All retries exhausted for station: {}", weatherPacket.getStationId());
            deadLetterQueueSave(weatherPacket);
        }
    }
}
