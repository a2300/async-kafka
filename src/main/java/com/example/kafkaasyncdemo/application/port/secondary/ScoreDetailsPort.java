package com.example.kafkaasyncdemo.application.port.secondary;

import com.example.kafkaasyncdemo.application.port.secondary.model.ScoreDetailsEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ScoreDetailsPort extends JpaRepository<ScoreDetailsEntity, Long> {

    default void save(String studentId, String subject, Double avgScore) {
        final var entity = new ScoreDetailsEntity(studentId, subject, avgScore);
        this.save(entity);
    }

    List<ScoreDetailsEntity> findAllByStudentId(String studentId);
}
