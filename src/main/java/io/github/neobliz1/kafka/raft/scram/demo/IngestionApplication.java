package io.github.neobliz1.kafka.raft.scram.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The main class for the ingestion application.
 */
@SpringBootApplication
public class IngestionApplication {

    /**
     * The main method for the ingestion application.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        SpringApplication.run(IngestionApplication.class, args);
    }
}
