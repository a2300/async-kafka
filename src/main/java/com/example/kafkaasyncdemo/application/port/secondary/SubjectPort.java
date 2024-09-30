package com.example.kafkaasyncdemo.application.port.secondary;

import com.example.kafkaasyncdemo.application.port.secondary.model.SubjectType;

public interface SubjectPort {

    Double getAverageScore(String studentId);

    SubjectType getSubjectType();
}
