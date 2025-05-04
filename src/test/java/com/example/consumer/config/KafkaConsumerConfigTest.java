package com.example.consumer.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestConstructor;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=test-group"
})
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"app-topic"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
public class KafkaConsumerConfigTest {

    @Autowired
    private KafkaConsumerConfig kafkaConsumerConfig;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void testConsumerFactory() {
        ConsumerFactory<String, String> consumerFactory = kafkaConsumerConfig.consumerFactory();
        Map<String, Object> configs = consumerFactory.getConfigurationProperties();

        assertThat(configs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo(embeddedKafkaBroker.getBrokersAsString());
        assertThat(configs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(configs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
    }

    @Test
    void testConcurrentKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = kafkaConsumerConfig.concurrentKafkaListenerContainerFactory();
        ConsumerFactory<String, String> consumerFactory = factory.getConsumerFactory();
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        assertThat(consumer.groupMetadata().groupId()).isEqualTo("grp-spring");
        consumer.close();
        assertThat(factory.getConcurrency()).isEqualTo(3);
        assertThat(factory.getRecordFilterStrategy()).isNotNull();
    }
}