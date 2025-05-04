package com.example.consumer.listener;

import com.example.consumer.config.TestConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Spy;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestConstructor;

import java.util.Map;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.group-id=test-group"
})
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"app-topic"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
@Import(TestConfig.class)
public class DetailListenerTest {

    @Spy
    private DetailListener detailListener;

    private final EmbeddedKafkaBroker embeddedKafkaBroker;

    private Producer<String, String> producer;

    public DetailListenerTest(EmbeddedKafkaBroker embeddedKafkaBroker) {
        this.embeddedKafkaBroker = embeddedKafkaBroker;
    }

    @BeforeAll
    void setUp() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producer = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer()).createProducer();
    }

    @AfterAll
    void tearDown() {
        producer.close();
    }

    @Test
    void testDetailListener() throws Exception {
        // Send a test message
        String key = "test-key";
        String value = "test-message";
        producer.send(new ProducerRecord<>("app-topic", key, value)).get();

        // Verify that the listener processed the message
        verify(detailListener, timeout(10000)).listenerDetails(org.mockito.Mockito.any());

        System.out.println("[DEBUG_LOG] DetailListener test passed successfully!");
    }
}