package io.github.neobliz1.kafka.raft.scram.demo.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Represents an Outbox entity for storing messages before they are sent to Kafka.
 * This is used in the transactional outbox pattern to ensure atomicity between
 * local database transactions and message publishing.
 */
@Entity
@Getter
@Setter
public class Outbox {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String stationId;

    /**
     * The payload of the message, typically a serialized {@link io.github.neobliz1.kafka.raft.scram.demo.proto.WeatherPacket}.
     */
    @JdbcTypeCode(SqlTypes.VARBINARY)
    @Column(columnDefinition = "BYTEA")
    private byte[] payload;

    /**
     * The status of the outbox entry (e.g., "PENDING", "SENT").
     */
    private String status;
}
