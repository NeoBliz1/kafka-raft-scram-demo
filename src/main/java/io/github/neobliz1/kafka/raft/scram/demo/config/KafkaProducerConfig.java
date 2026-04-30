package io.github.neobliz1.kafka.raft.scram.demo.config;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.github.neobliz1.kafka.raft.scram.demo.producer.WeatherIngestionProducer;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.net.InetAddress;
import java.util.Map;

/**
 * Configuration class for Kafka producer.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;

    /**
     * Custom transaction ID prefix for Kafka producers, loaded from application properties.
     */
    @Value("${spring.kafka.producer.custom-transaction-id-prefix}")
    private String customKafkaTransactionPrefix;

    /**
     * The name of the Kafka topic, loaded from application properties.
     */
    @Value("${app.kafka.topic.name}")
    private String topicName;

    /**
     * Creates a Kafka producer factory with transactional capabilities.
     * This factory is active when the "test-transactions-off" profile is NOT active.
     * The transaction ID prefix is dynamically generated using the hostname.
     *
     * @return The Kafka producer factory configured for transactions.
     */
    @Bean
    @Profile("!test-transactions-off")
    public ProducerFactory<String, WeatherPacket> producerFactoryWithKafkaTransactions() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        DefaultKafkaProducerFactory<String, WeatherPacket> producerFactory = new DefaultKafkaProducerFactory<>(props);
        String resolvedHostname;
        try {
            resolvedHostname = InetAddress.getLocalHost().getHostName();
        } catch(java.net.UnknownHostException e) {
            resolvedHostname = "unknown-host";
        }
        String transactionIdPrefix = customKafkaTransactionPrefix+resolvedHostname+"-";
        producerFactory.setTransactionIdPrefix(transactionIdPrefix);
        return producerFactory;
    }

    /**
     * Creates a Kafka producer factory without transactional capabilities.
     * This factory is active when the "test-transactions-off" profile IS active.
     *
     * @return The Kafka producer factory without transactional configuration.
     */
    @Bean
    @Profile("test-transactions-off")
    public ProducerFactory<String, WeatherPacket> producerFactory() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Creates a Kafka template.
     *
     * @param producerFactory The Kafka producer factory to use.
     * @return The Kafka template.
     */
    @Bean
    public KafkaTemplate<String, WeatherPacket> kafkaTemplate(ProducerFactory<String, WeatherPacket> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Creates a weather ingestion producer with transactional capabilities.
     * This producer is active when the "test-transactions-off" profile is NOT active.
     *
     * @param kafkaTemplate The Kafka template configured for transactions.
     * @return The weather ingestion producer with transactional support.
     */
    @Bean
    @Profile("!test-transactions-off")
    public WeatherIngestionProducer weatherStationProducerWithKafkaTransaction(KafkaTemplate<String, WeatherPacket> kafkaTemplate) {
        return new WeatherIngestionProducer(kafkaTemplate);
    }

    /**
     * Creates a weather ingestion producer without transactional capabilities.
     * This producer is active when the "test-transactions-off" profile IS active.
     * It overrides the `sendTransactional` method to send messages non-transactionally.
     *
     * @param kafkaTemplate The Kafka template.
     * @return The weather ingestion producer without transactional support.
     */
    @Bean
    @Profile("test-transactions-off")
    public WeatherIngestionProducer weatherStationProducer(KafkaTemplate<String, WeatherPacket> kafkaTemplate) {
        return new WeatherIngestionProducer(kafkaTemplate) {
            @Override
            public RecordMetadata sendTransactional(WeatherPacket weatherPacket) {
                kafkaTemplate.send(topicName, weatherPacket.getStationId(), weatherPacket);
                return null;
            }
        };
    }
}
