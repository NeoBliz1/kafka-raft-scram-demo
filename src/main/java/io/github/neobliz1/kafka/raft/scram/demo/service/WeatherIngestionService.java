package io.github.neobliz1.kafka.raft.scram.demo.service;


import io.github.neobliz1.kafka.raft.scram.demo.producer.WeatherIngestionProducer;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

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
}
