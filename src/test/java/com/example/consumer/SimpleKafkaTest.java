package com.example.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"test-topic"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class SimpleKafkaTest {

    private static final String TEST_TOPIC = "test-topic";

    private final EmbeddedKafkaBroker embeddedKafkaBroker;

    public SimpleKafkaTest(EmbeddedKafkaBroker embeddedKafkaBroker) {
        this.embeddedKafkaBroker = embeddedKafkaBroker;
    }

    private Producer<String, String> producer;
    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        // Configure the producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producer = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer()).createProducer();

        // Configure the consumer
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer()).createConsumer();
        consumer.subscribe(Collections.singletonList(TEST_TOPIC));
    }

    @AfterEach
    void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    void testSendAndReceiveMessage() throws Exception {
        // Send a test message
        String key = "test-key";
        String value = "test-message";
        producer.send(new ProducerRecord<>(TEST_TOPIC, key, value)).get();

        // Consume the message
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, TEST_TOPIC, Duration.ofSeconds(10));

        // Verify the message
        assertNotNull(record);
        assertEquals(key, record.key());
        assertEquals(value, record.value());

        System.out.println("[DEBUG_LOG] Simple Kafka test passed successfully!");
    }
}
