package com.example.consumer.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Component
public class FilteredListener {

    private Logger logger = LoggerFactory.getLogger(HeaderListener.class);

    @KafkaListener(groupId = "spring4",
        topicPartitions = @TopicPartition(
                topic="app-topic",
//                partitions = {"4", "6"}
                partitionOffsets = @PartitionOffset(
                        partition = "4",
                        initialOffset = "2"
                )
        ))
    public void filteredListenr(String message){
        logger.info("filtered message : "+ message);
    }
}
