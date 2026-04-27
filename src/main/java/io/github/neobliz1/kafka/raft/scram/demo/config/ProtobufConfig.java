package io.github.neobliz1.kafka.raft.scram.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;

/**
 * Configuration class for Protobuf message conversion.
 */
@Configuration
public class ProtobufConfig {

    /**
     * Creates a ProtobufHttpMessageConverter bean.
     *
     * @return The ProtobufHttpMessageConverter bean.
     */
    @Bean
    public ProtobufHttpMessageConverter protobufHttpMessageConverter() {
        return new ProtobufHttpMessageConverter();
    }
}
