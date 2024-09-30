package com.example.kafkaasyncdemo.adapter.secondary;

import com.example.kafkaasyncdemo.application.port.secondary.SubjectPort;
import com.example.kafkaasyncdemo.application.port.secondary.model.SubjectType;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Random;

import static com.example.kafkaasyncdemo.application.port.secondary.model.SubjectType.MATH;
import static java.time.temporal.ChronoUnit.SECONDS;

@Service
public class MathSubjectAdapter implements SubjectPort {

    @Override
    public SubjectType getSubjectType() {
        return MATH;
    }

    @Override
    public Double getAverageScore(String studentId) {
        try {
            Thread.sleep(Duration.of(new Random().nextLong(1, 10), SECONDS).toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return new Random().nextDouble(1, 5);
    }
}
