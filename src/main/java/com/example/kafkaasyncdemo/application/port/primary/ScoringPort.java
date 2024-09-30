package com.example.kafkaasyncdemo.application.port.primary;

public interface ScoringPort {

    Double getScore(String studentId);
}
