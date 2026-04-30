package io.github.neobliz1.kafka.raft.scram.demo.base;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

/**
 * Base test class for integration tests that require ToxiProxy for simulating network issues.
 * This class sets up a Docker Compose environment with Kafka, Schema Registry, and ToxiProxy,
 * and provides utilities for managing the proxy and its toxics.
 */
@Slf4j
@Testcontainers(disabledWithoutDocker = true)
public abstract class BaseToxyProxyTestCase extends BaseKafkaTestCase {

    public static final String TOXIPROXY = "toxiproxy";
    public static final int TOXI_PORT_1 = 8474; // API
    public static final int TOXI_PORT_2 = 9094; // Proxy

    protected static ToxiproxyClient TOXIPROXY_CLIENT;
    protected static Proxy KAFKA_PROXY;
    protected static String PROXY_BOOTSTRAP_ADDRESS;

    protected static void setupToxiproxy(ComposeContainer env) {
        int apiPort = env.getServicePort(TOXIPROXY, TOXI_PORT_1);
        int proxyPort = env.getServicePort(TOXIPROXY, TOXI_PORT_2);

        // Ensure API is ready
        await().atMost(30, SECONDS).until(() -> {
            try {
                TOXIPROXY_CLIENT = new ToxiproxyClient("localhost", apiPort);
                TOXIPROXY_CLIENT.version();
                return true;
            } catch(Exception e) {
                return false;
            }
        });

        try {
            String proxyName = "kafka_proxy";
            Proxy existing = TOXIPROXY_CLIENT.getProxyOrNull(proxyName);
            if(existing!=null) existing.delete();

            KAFKA_PROXY = TOXIPROXY_CLIENT.createProxy(proxyName, "0.0.0.0:"+TOXI_PORT_2, "kafka:9092");
            PROXY_BOOTSTRAP_ADDRESS = "localhost:"+proxyPort;

            log.info("Toxiproxy ready. Proxy Address: {}", PROXY_BOOTSTRAP_ADDRESS);
        } catch(IOException e) {
            throw new RuntimeException("Toxiproxy setup failed", e);
        }
    }

    @BeforeEach
    @Override
    void setUp() throws Exception {
        clearToxics();
        super.setUp();
    }

    @AfterEach
    @Override
    void tearDown() {
        super.tearDown();
        clearToxics();
    }

    private void clearToxics() {
        if(KAFKA_PROXY!=null) {
            try {
                for(Toxic t : KAFKA_PROXY.toxics().getAll()) {
                    t.remove();
                }
            } catch(Exception e) {
                log.warn("Cleanup failed: {}", e.getMessage());
            }
        }
    }

    @Override
    protected String getBootstrapServers() {
        return PROXY_BOOTSTRAP_ADDRESS;
    }
}