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
import java.util.concurrent.ExecutionException;

/**
 * Configuration class for Kafka producer.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;

    @Value("${spring.kafka.producer.custom-transaction-id-prefix}")
    private String customKafkaTransactionPrefix;

    @Value("${app.kafka.topic.name}")
    private String topicName;

    /**
     * Creates a Kafka producer factory.
     *
     * @return The Kafka producer factory.
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
     * @return The Kafka template.
     */
    @Bean
    public KafkaTemplate<String, WeatherPacket> kafkaTemplate(ProducerFactory<String, WeatherPacket> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Creates a weather ingestion producer.
     *
     * @param kafkaTemplate The Kafka template.
     * @return The weather ingestion producer.
     */
    @Bean
    @Profile("!test-transactions-off")
    public WeatherIngestionProducer weatherStationProducerWithKafkaTransaction(KafkaTemplate<String, WeatherPacket> kafkaTemplate) {
        return new WeatherIngestionProducer(kafkaTemplate);
    }

    @Bean
    @Profile("test-transactions-off")
    public WeatherIngestionProducer weatherStationProducer(KafkaTemplate<String, WeatherPacket> kafkaTemplate) {
        return new WeatherIngestionProducer(kafkaTemplate) {
            @Override
            public RecordMetadata sendTransactional(WeatherPacket weatherPacket)
                    throws ExecutionException, InterruptedException {
                return kafkaTemplate.send(topicName, weatherPacket.getStationId(), weatherPacket)
                        .get()
                        .getRecordMetadata();
            }
        };
    }
}
