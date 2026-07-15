package minchakov.arkadii.emailnotification.repository;

import minchakov.arkadii.emailnotification.model.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
