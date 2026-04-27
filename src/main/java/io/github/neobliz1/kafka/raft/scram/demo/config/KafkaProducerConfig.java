package io.github.neobliz1.kafka.raft.scram.demo.config;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.github.neobliz1.kafka.raft.scram.demo.producer.WeatherIngestionProducer;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.resilience.retry.MethodRetryEvent;

import java.util.Map;

/**
 * Configuration class for Kafka producer.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;

    /**
     * Creates a Kafka producer factory.
     *
     * @return The Kafka producer factory.
     */
    @Bean
    public ProducerFactory<String, WeatherPacket> producerFactory() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Creates a Kafka template.
     *
     * @return The Kafka template.
     */
    @Bean
    public KafkaTemplate<String, WeatherPacket> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Creates a weather ingestion producer.
     *
     * @param kafkaTemplate The Kafka template.
     * @return The weather ingestion producer.
     */
    @Bean
    public WeatherIngestionProducer weatherStationProducer(KafkaTemplate<String, WeatherPacket> kafkaTemplate) {
        return new WeatherIngestionProducer(kafkaTemplate);
    }

    /**
     * Handles retry events.
     *
     * @param event The method retry event.
     */
    @EventListener
    public void onRetry(MethodRetryEvent event) {
        // This triggers for EVERY failed attempt
        System.out.println("Retry attempt detected for method: "+event.getMethod().getName());
        System.out.println("Exception: "+event.getFailure().getMessage());

        // You can check if the retry was eventually aborted (failed all attempts)
        if(event.isRetryAborted()) {
            System.err.println("Retry exhausted! No more attempts.");
        }
    }
}
