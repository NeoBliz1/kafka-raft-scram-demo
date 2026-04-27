package io.github.neobliz1.kafka.raft.scram.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = { "${app.kafka.topic.name}" }
)
@SpringBootTest(
        classes = IngestionApplication.class,
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                // 2. Use the mock schema registry protocol for the producer
                "spring.kafka.producer.properties.schema.registry.url=mock://test-url"
        }
)
class WeatherIngestionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Value("${app.kafka.topic.name}")
    private String topicName;

    private Consumer<String, WeatherPacket> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer.class);
        consumerProps.put("schema.registry.url", "mock://test-url"); // Mock schema registry for deserialization
        consumerProps.put("specific.protobuf.value.type", WeatherPacket.class.getName());

        DefaultKafkaConsumerFactory<String, WeatherPacket> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topicName);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void shouldIngestWeatherDataAndSendToKafka() throws Exception {
        // 1. Create sensor data
        WeatherPacket weatherPacket = WeatherPacket.newBuilder()
                .setStationId("station-123")
                .setTimestamp(Instant.now().toEpochMilli())
                .build();

        // 2. Send it with MockMvc to the controller
        mockMvc.perform(post("/api/v1/weather")
                        .contentType(MediaType.APPLICATION_PROTOBUF_VALUE)
                        .content(weatherPacket.toByteArray()))
                .andExpect(status().isAccepted());

        // 3. Consume the message and verify it
        ConsumerRecords<String, WeatherPacket> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));

        assertThat(records.count()).isEqualTo(1);
        WeatherPacket consumedPacket = records.iterator().next().value();
        assertThat(consumedPacket).isNotNull();
        assertThat(consumedPacket.getStationId()).isEqualTo("station-123");
    }
}
