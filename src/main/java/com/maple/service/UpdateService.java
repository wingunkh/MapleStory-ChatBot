package com.maple.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.maple.domain.ClientUpdate;
import com.maple.repository.UpdateRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
@RequiredArgsConstructor
public class UpdateService extends InformationService {
    @Value("${api.key}")
    private String key;
    private static final String API_URL = "https://open.api.nexon.com/maplestory/v1/notice-update";
    private final RestTemplate restTemplate;
    private final UpdateRepository updateRepository;

    @Transactional
    @CacheEvict(value = "myCache", allEntries = true)
    public void fetchUpdates() {
        ResponseEntity<String> httpResponse = sendHttpRequest(key, restTemplate, API_URL);

        JsonNode clientUpdateNodes = parseJsonToJsonNode(httpResponse.getBody(), "update_notice");

        List<ClientUpdate> clientUpdates = new ArrayList<>();

        for (int i = 0; i < Math.min(clientUpdateNodes.size(), 10); i++) {
            JsonNode clientUpdateNode = clientUpdateNodes.get(i);
            ClientUpdate clientUpdate = new ClientUpdate();
            clientUpdate.setId(clientUpdateNode.get("notice_id").asLong());
            clientUpdate.setTitle(clientUpdateNode.get("title").asText());
            clientUpdate.setUrl(clientUpdateNode.get("url").asText());
            clientUpdate.setDate(ClientUpdate.convertTime(String.valueOf(clientUpdateNode.get("date"))));
            clientUpdate.setLocalDateTime(LocalDateTime.now());
            clientUpdates.add(clientUpdate);
        }

        updateRepository.deleteAll();
        updateRepository.saveAll(clientUpdates);
    }

    @Cacheable(value = "myCache", key = "'update'")
    public HashMap<String, Object> findAllUpdate() {
        HashMap<String, Object> jsonData = createJsonData();

        HashMap<String, Object> simpleText = extractSimpleText(jsonData);

        StringBuilder result = new StringBuilder();

        List<ClientUpdate> clientUpdates = updateRepository.findAll(Sort.by(Sort.Direction.DESC, "id"));

        if (clientUpdates.isEmpty()) {
            throw new RuntimeException();
        }

        result.append("(").append(clientUpdates.get(0).getLocalDateTime().toLocalDate()).append(" 오전 03:00 기준)").append("\n\n");

        for (ClientUpdate clientUpdate : clientUpdates) {
            result.append(
                    String.join("\n",
                            "▶ " + clientUpdate.getTitle(),
                            "☞ 업데이트 링크: " + clientUpdate.getUrl(),
                            "☞ 업데이트 날짜: " + clientUpdate.getDate()
                    )
            ).append("\n\n");
        }

        simpleText.put("text", result.toString());

        return jsonData;
    }
}
