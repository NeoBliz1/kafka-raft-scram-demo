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
            weatherIngestionProducer.deadLetterQueueSave(weatherPacket);
        }
    }
}
