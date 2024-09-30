package com.example.kafkaasyncdemo;

import com.example.kafkaasyncdemo.adapter.primary.kafka.ExampleProducer;
import com.example.kafkaasyncdemo.application.port.secondary.ScoreDetailsPort;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

public class CountDownLatchWithSpyBeanTest extends BaseServiceTest {

    // client producer imitation
    private static KafkaTemplate<String, String> clientProducer;

    @Autowired
    private ScoreDetailsPort scoreDetailsPort;

    @SpyBean
    private ExampleProducer exampleProducer;

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
        final var countDownLatch = new CountDownLatch(1);

        // как только вызывается response producer, вызываем countDown(), чтобы пустить тестовый поток к блоку проверок
        // тем самым гарантируем, что проверка начнется после того, как отправлен ответ клиенту
        doAnswer(invocation -> {
            countDownLatch.countDown();
            return invocation.callRealMethod();
        }).when(exampleProducer).sendMessage(anyString(), anyDouble());

        // имитация клиентского запроса
        final var studentId = "123";
        clientProducer.send("request-topic", "key", studentId);

        // блочим тестовый поток до тех пор, пока не вызван countDown()
        countDownLatch.await(15, TimeUnit.SECONDS);

        final var details = scoreDetailsPort.findAllByStudentId(studentId);
        final var stringArgumentCaptor = ArgumentCaptor.forClass(String.class);
        final var doubleArgumentCaptor = ArgumentCaptor.forClass(Double.class);
        Mockito.verify(exampleProducer).sendMessage(stringArgumentCaptor.capture(), doubleArgumentCaptor.capture());
        final var expectedScore = doubleArgumentCaptor.getValue();
        final var expectedStudentId = stringArgumentCaptor.getValue();

        assertThat(details).as("проверка артефактов БД").hasSize(2);
        assertThat(expectedStudentId).as("проверка ID студента").isEqualTo(studentId);
        assertThat(expectedScore).as("проверка наличия оценки").isNotNull();
    }
}
