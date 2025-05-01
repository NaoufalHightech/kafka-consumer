package com.example.consumer.config;

import com.example.consumer.listener.BasicListener;
import com.example.consumer.listener.DetailListener;
import com.example.consumer.listener.FilteredListener;
import com.example.consumer.listener.HeaderListener;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class TestConfig {

    @Bean
    @Primary
    public BasicListener basicListener() {
        return new BasicListener();
    }

    // Provide no-op implementations of the other listeners to prevent errors
    @Bean
    @Primary
    public DetailListener detailListener() {
        return new DetailListener() {
            @Override
            public void listenerDetails(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
                // Do nothing - this is a no-op implementation for testing
            }
        };
    }

    @Bean
    @Primary
    public HeaderListener headerListener() {
        return new HeaderListener() {
            @Override
            public void headerListener(String message, String key, int offset, String timeRecieved) {
                // Do nothing - this is a no-op implementation for testing
            }
        };
    }

    @Bean
    @Primary
    public FilteredListener filteredListener() {
        return new FilteredListener() {
            @Override
            public void filteredListenr(String message) {
                // Do nothing - this is a no-op implementation for testing
            }
        };
    }
}
