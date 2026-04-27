package io.github.neobliz1.kafka.raft.scram.demo;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:application.yml")
public abstract class BaseKafkaTestCase {

    public static final String API_V_1_WEATHER = "/api/v1/weather";
    public static final String KAFKA = "kafka";
    public static final int KAFKA_PORT = 9092;
    public static final String SCHEMA_REGISTRY = "schema-registry";
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    @Autowired
    protected MockMvc mockMvc;

    @Value("${app.kafka.topic.name}")
    protected String topicName;
    protected Consumer<String, WeatherPacket> consumer;

    protected abstract String getBootstrapServers();

    protected abstract String getSchemaRegistryUrl();

    @BeforeEach
    void setUp() throws Exception {
        String bootstrap = getBootstrapServers();
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            try(var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
                admin.listTopics().names().get(10, TimeUnit.SECONDS);
            }
        });
        cleanupAndCreateTopic(bootstrap);
        setupConsumer(bootstrap, getSchemaRegistryUrl());
    }

    private void cleanupAndCreateTopic(String bootstrap) throws Exception {
        try(var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            var topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            if(topics.contains(topicName)) {
                admin.deleteTopics(Collections.singletonList(topicName)).all().get(10, TimeUnit.SECONDS);
                Thread.sleep(3000);
            }
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            newTopic.configs(Map.of("min.insync.replicas", "1", "retention.ms", "60000"));
            admin.createTopics(Collections.singletonList(newTopic)).all().get(10, TimeUnit.SECONDS);
            Thread.sleep(3000);
        }
    }

    private void setupConsumer(String bootstrap, String registryUrl) {
        if(consumer!=null) {
            try {
                consumer.close();
            } catch(Exception ignored) {
            }
        }
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-"+UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.protobuf.value.type", WeatherPacket.class.getName());
        consumer = new DefaultKafkaConsumerFactory<String, WeatherPacket>(consumerProps).createConsumer();
        consumer.subscribe(Collections.singleton(topicName));
        consumer.poll(Duration.ofMillis(500));
        consumer.commitSync();
    }

    @NotNull
    public static String getStationId(String batchId) {
        return "station-"+batchId;
    }

    public void sendWeatherPacket(String batchId, long index) throws Exception {
        long start = System.currentTimeMillis();
        WeatherPacket packet = WeatherPacket.newBuilder()
                .setStationId(getStationId(batchId))
                .setTimestamp(index)
                .build();
        long buildTime = System.currentTimeMillis()-start;

        long beforeSend = System.currentTimeMillis();
        mockMvc.perform(post(API_V_1_WEATHER)
                        .contentType(MediaType.APPLICATION_PROTOBUF)
                        .content(packet.toByteArray()))
                .andExpect(status().is2xxSuccessful());
        long sendTime = System.currentTimeMillis()-beforeSend;

        log.info("Message {}: build={}ms, send={}ms", index, buildTime, sendTime);
    }

    @AfterEach
    void tearDown() {
        if(consumer!=null) {
            try {
                consumer.close();
            } catch(Exception ignored) {
            }
        }
    }
}
