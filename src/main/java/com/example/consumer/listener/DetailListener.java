package com.example.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DetailListener {

    private Logger logger = LoggerFactory.getLogger(DetailListener.class);

    @KafkaListener(topics = "app-topic", groupId = "spring2")
    public void listenerDetails(ConsumerRecord<String, String> record){
        logger.info(" details message : "+ record.value());
        logger.info(" with key : "+ record.key());
    }
}
