package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);;


    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
