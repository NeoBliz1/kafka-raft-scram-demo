package io.github.neobliz1.kafka.raft.scram.demo.base;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;

@Slf4j
@Testcontainers(disabledWithoutDocker = true)
public abstract class BaseToxyProxyTestCase extends BaseKafkaTestCase {

    public static final String TOXIPROXY = "toxiproxy";
    public static final int TOXI_PORT_1 = 8474;
    public static final int TOXI_PORT_2 = 9094;
    protected static ToxiproxyClient TOXIPROXY_CLIENT;
    protected static Proxy KAFKA_PROXY;
    protected static String PROXY_BOOTSTRAP_ADDRESS;

    @org.testcontainers.junit.jupiter.Container
    static ComposeContainer ENVIRONMENT = new ComposeContainer(new File("kafka/docker-compose-toxy-proxy.yml"))
            .withExposedService(TOXIPROXY, TOXI_PORT_1, Wait.forListeningPort())
            .withExposedService(KAFKA, KAFKA_PORT, Wait.forListeningPort())
            .withExposedService(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT, Wait.forHttp("/subjects")
                    .forPort(SCHEMA_REGISTRY_PORT).forStatusCode(200))
            .withExposedService(TOXIPROXY, TOXI_PORT_2, Wait.forListeningPort());


    @BeforeAll
    static void setupToxiproxy() throws Exception {
        await().atMost(60, SECONDS).pollInterval(2, SECONDS).until(() -> {
            try {
                int toxiproxyApiPort = ENVIRONMENT.getServicePort(TOXIPROXY, TOXI_PORT_1);
                TOXIPROXY_CLIENT = new ToxiproxyClient("localhost", toxiproxyApiPort);
                String version = TOXIPROXY_CLIENT.version();
                log.info("Toxiproxy API is ready, version: {}", version);
                return true;
            } catch(Exception e) {
                log.warn("Waiting for Toxiproxy API: {}", e.getMessage());
                return false;
            }
        });

        String kafkaInternalAddress = "kafka:9092";
        String proxyName = "kafka_proxy";
        Proxy existingProxy = TOXIPROXY_CLIENT.getProxyOrNull(proxyName);
        if(existingProxy!=null) {
            existingProxy.delete();
        }
        KAFKA_PROXY = TOXIPROXY_CLIENT.createProxy(proxyName, "0.0.0.0:"+TOXI_PORT_2, kafkaInternalAddress);

        KAFKA_PROXY.toxics().timeout("kafka_timeout", ToxicDirection.UPSTREAM, 0).setTimeout(0);

        int proxyPort = ENVIRONMENT.getServicePort(TOXIPROXY, TOXI_PORT_2);
        PROXY_BOOTSTRAP_ADDRESS = "localhost:"+proxyPort;
        Thread.sleep(2000);
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> PROXY_BOOTSTRAP_ADDRESS);
        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> "http://localhost:"+ENVIRONMENT.getServicePort(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT));
        registry.add("spring.kafka.admin.properties.bootstrap.servers", () -> PROXY_BOOTSTRAP_ADDRESS);
    }

    @BeforeEach
    @Override
    void setUp() throws Exception {
        Thread.sleep(5000);
        KAFKA_PROXY.toxics().get("kafka_timeout").remove();
        Thread.sleep(5000);
        super.setUp();
    }

    @AfterEach
    @Override
    void tearDown() {
        super.tearDown();
        try {
            KAFKA_PROXY.toxics().get("kafka_timeout").remove();
        } catch(Exception e) {
            // ignore
        }
    }

    @Override
    protected String getBootstrapServers() {
        return PROXY_BOOTSTRAP_ADDRESS;
    }

    @Override
    protected String getSchemaRegistryUrl() {
        return "http://localhost:"+ENVIRONMENT.getServicePort(SCHEMA_REGISTRY, SCHEMA_REGISTRY_PORT);
    }
}
