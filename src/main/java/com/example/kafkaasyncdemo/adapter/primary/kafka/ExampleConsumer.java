package com.example.kafkaasyncdemo.adapter.primary.kafka;

import com.example.kafkaasyncdemo.application.port.primary.ScoringPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ExampleConsumer {

    Logger logger = LoggerFactory.getLogger(ExampleConsumer.class);

    private final ScoringPort scoringPort;
    private final ExampleProducer producer;

    public ExampleConsumer(ScoringPort scoringPort, ExampleProducer producer) {
        this.scoringPort = scoringPort;
        this.producer = producer;
    }

    @KafkaListener(topics = "request-topic")
    public void listen(String studentId) {
        logger.info("Received studentId: {}", studentId);

        try {
            final var score = scoringPort.getScore(studentId);
            producer.sendMessage(studentId, score);
        } catch (Throwable throwable) {
            logger.error("Error processing request with studentId: {}", studentId, throwable);
        }
    }
}
