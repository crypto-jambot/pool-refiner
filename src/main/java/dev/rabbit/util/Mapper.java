package dev.rabbit.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Mapper {
    public static final Mapper INSTANCE = new Mapper();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, Object> readJson(String json) throws JsonProcessingException {
        return objectMapper.readValue(json, Map.class);
    }
}
