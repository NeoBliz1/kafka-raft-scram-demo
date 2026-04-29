package io.github.neobliz1.kafka.raft.scram.demo.repository;

import io.github.neobliz1.kafka.raft.scram.demo.domain.Outbox;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * Spring Data JPA repository for {@link Outbox} entities.
 * Provides methods for querying outbox entries, particularly for finding
 * entries with a specific status or by station ID and status.
 */
public interface OutboxRepository extends JpaRepository<Outbox, Long> {
    List<Outbox> findByStatus(String status);

    Outbox findByStationIdAndStatus(String stationId, String pending);
}
