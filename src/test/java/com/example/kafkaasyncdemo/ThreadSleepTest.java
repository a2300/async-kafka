package com.example.kafkaasyncdemo;

import com.example.kafkaasyncdemo.application.port.secondary.ScoreDetailsPort;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class ThreadSleepTest extends BaseServiceTest {

    // client producer imitation
    private static KafkaTemplate<String, String> clientProducer;

    @Autowired
    private ScoreDetailsPort scoreDetailsPort;

    @BeforeAll
    static void setUp() {
        // create imitation for client producer
        final var clientProducerProps = KafkaTestUtils.producerProps(KAFKA_CONTAINER.getBootstrapServers());
        clientProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        clientProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        clientProducer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(clientProducerProps));
    }

    @Test
    void example() throws InterruptedException {
        // имитация клиентского запроса
        clientProducer.send("request-topic", "key", "123");

        // усыпляем тестовый поток
        Thread.sleep(15_000);

        // проверяем артефакты БД
        final var details = scoreDetailsPort.findAll();
        assertThat(details).as("проверка артефактов БД").hasSize(2);
    }
}
