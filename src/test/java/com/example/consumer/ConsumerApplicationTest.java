package com.example.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"app-topic"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class ConsumerApplicationTest {

    @Test
    void contextLoads() {
        assertTrue(true);
    }
}