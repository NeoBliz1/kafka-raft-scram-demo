package io.github.neobliz1.kafka.raft.scram.demo.controller;

import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import io.github.neobliz1.kafka.raft.scram.demo.service.WeatherIngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for ingesting weather data.
 * This controller exposes an endpoint for receiving {@link WeatherPacket}
 * and delegates the processing to {@link WeatherIngestionService}.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/weather")
public class WeatherIngestionController {

    private final WeatherIngestionService weatherIngestionService;

    /**
     * Ingests a weather packet received via a POST request.
     * The request body should be in Protobuf format.
     *
     * @param weatherPacket The {@link WeatherPacket} to ingest.
     * @return A {@link ResponseEntity} indicating the acceptance of the request.
     */
    @PostMapping(consumes = MediaType.APPLICATION_PROTOBUF_VALUE)
    public ResponseEntity<Void> ingest(@RequestBody WeatherPacket weatherPacket) {
        log.info("Received weather data: {}", weatherPacket);
        weatherIngestionService.sendWeatherPacket(weatherPacket);
        return ResponseEntity.accepted().build();
    }
}
