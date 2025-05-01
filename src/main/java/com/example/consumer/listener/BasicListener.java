package com.example.consumer.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BasicListener {

    private Logger logger = LoggerFactory.getLogger(BasicListener.class);

    @KafkaListener(topics = "app-topic", groupId = "spring1")
    public void listener(String message){
        logger.info("Basic message : "+ message);
    }
}
