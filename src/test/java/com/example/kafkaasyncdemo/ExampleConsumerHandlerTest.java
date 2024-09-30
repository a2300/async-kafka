package com.example.kafkaasyncdemo;

import com.example.kafkaasyncdemo.adapter.primary.kafka.model.ScoreResult;
import com.example.kafkaasyncdemo.application.port.secondary.ScoreDetailsPort;
import com.example.kafkaasyncdemo.application.port.secondary.model.ScoreDetailsEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests have been written based on guidance provided by a tutorial.
 * The tutorial can be found at:
 * <a href="https://testcontainers.com/guides/testing-spring-boot-kafka-listener-using-testcontainers/">
 *     Testing Spring Boot Kafka Listener using Testcontainers</a>.
 */
@TestPropertySource(properties = "spring.kafka.consumer.auto-offset-reset=earliest")
public class ExampleConsumerHandlerTest extends BaseServiceTest {
    protected static final long AWAITILITY_DEFAULT_WAIT_AT_MOST_MS = 10000;
    protected static final long AWAITILITY_DEFAULT_POLL_INTERVAL_MS = 500;
    private static KafkaTemplate<String, String> clientProducer;
    private static DefaultKafkaConsumerFactory clientConsumerFactory;

    @Autowired
    private ScoreDetailsPort scoreDetailsPort;
    @Autowired
    private ObjectMapper objectMapper;

    @BeforeAll
    static void setUp() {
        // create imitation for client producer
        final var clientProducerProps = KafkaTestUtils.producerProps(KAFKA_CONTAINER.getBootstrapServers());
        clientProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        clientProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        clientProducer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(clientProducerProps));

        // создаем имитацию клиентского консюмера
        final var clientConsumerProps = new HashMap<String, Object>();
        clientConsumerProps.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        clientConsumerProps.put(GROUP_ID_CONFIG, "consumer");
        clientConsumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        clientConsumerProps.put(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        clientConsumerProps.put(MAX_POLL_RECORDS_CONFIG, "1");
        clientConsumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        clientConsumerProps.put(MAX_POLL_INTERVAL_MS_CONFIG, "15000"); // больше времени ответа сервиса

        final var deserializer = new JsonDeserializer<ScoreResult>();
        deserializer.addTrustedPackages("*");
        clientConsumerFactory = new DefaultKafkaConsumerFactory<>(
                clientConsumerProps,
                new StringDeserializer(),
                deserializer
        );
    }

    @Test
    void sendMessageTest() {
        final var studentId = "123";
        clientProducer.send("request-topic", "key", studentId);

        await()
                .pollInterval(Duration.ofSeconds(3))
                .atMost(15, SECONDS)
                .untilAsserted(() -> {
                    final var details = scoreDetailsPort.findAllByStudentId(studentId);

                    assertThat(details).as("проверка артефактов БД").hasSize(2);
                    assertThat(details).map(ScoreDetailsEntity::getStudentId).allMatch(s -> s.equals(studentId));
                    assertThat(details).map(ScoreDetailsEntity::getSubject)
                            .containsExactlyInAnyOrder("CHEMISTRY", "MATH");
                    assertThat(details).map(ScoreDetailsEntity::getAvgScore).isNotNull();
                });
    }

    @Test
    void sendAndReceiveMessageTest() {
        // given
        final var studentId = "123";
        clientProducer.send("request-topic", "key", studentId);

        // when
        final ConsumerRecord<String, ScoreResult> kafkaRecord = awaitForKafkaMessage("response-topic");

        // then
        assertThat(kafkaRecord.key()).isNull();
        assertThat(kafkaRecord.value().studentId()).isEqualTo(studentId);
        assertThat(kafkaRecord.value().avgScore()).isGreaterThan(0);
    }

    protected <T> ConsumerRecord<String, T> awaitForKafkaMessage(String topic) {
        return (ConsumerRecord<String, T>) awaitForKafkaMessages(
                1,
                topic,
                Duration.ofMillis(AWAITILITY_DEFAULT_WAIT_AT_MOST_MS),
                Duration.ofMillis(AWAITILITY_DEFAULT_POLL_INTERVAL_MS)).iterator().next();
    }

    protected <T> ConsumerRecords<String, T> awaitForKafkaMessages(
            int expectedCount,
            String topic,
            Duration waitAtMost,
            Duration pollInterval) {
        final var consumerRecords = new AtomicReference<ConsumerRecords<String, T>>();
        try (Consumer<String, T> consumer = clientConsumerFactory.createConsumer()) {
            consumer.subscribe(List.of(topic));
            Awaitility.waitAtMost(waitAtMost)
                    .pollInterval(pollInterval)
                    .untilAsserted(() -> {
                        final ConsumerRecords<String, T> records = consumer.poll(pollInterval);
                        Assertions.assertThat(records).hasSize(expectedCount);
                        consumerRecords.set(records);
                    });
        }
        return consumerRecords.get();
    }
}
