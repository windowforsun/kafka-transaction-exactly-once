package com.windowforsun.kafka.transaction.service;

import com.windowforsun.kafka.transaction.exception.KafkaDemoException;
import com.windowforsun.kafka.transaction.exception.KafkaDemoRetryableException;
import com.windowforsun.kafka.transaction.lib.KafkaClient;
import com.windowforsun.kafka.transaction.event.DemoInboundEvent;
import com.windowforsun.kafka.transaction.properties.KafkaDemoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Service
@RequiredArgsConstructor
public class DemoService {
    private final KafkaDemoProperties properties;
    private final KafkaClient kafkaClient;

    public void processWithoutTransaction(String key, DemoInboundEvent event) {
        this.kafkaClient.sendMessageWithoutTransaction(key, event.getData(), this.properties.getOutboundTopic1());
        this.callThirdparty(key);
        this.kafkaClient.sendMessageWithoutTransaction(key, event.getData(), this.properties.getOutboundTopic2());
    }

    @Transactional
    public void processWithTransaction(String key, DemoInboundEvent event) {
        this.kafkaClient.sendMessageWithTransaction(key, event.getData(), this.properties.getOutboundTopic1());
        this.callThirdparty(key);
        this.kafkaClient.sendMessageWithTransaction(key, event.getData(), this.properties.getOutboundTopic2());
    }

    private void callThirdparty(String key) {
        RestTemplate restTemplate = new RestTemplate();
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(this.properties.getThirdpartyEndpoint() + "/" + key, String.class);

            if(response.getStatusCodeValue() != 200) {
                throw new RuntimeException("error " + response.getStatusCodeValue());
            }
        } catch (HttpServerErrorException e) {
            log.error("Retryable Error calling thirdparty api, returned an (" + e.getClass().getName() + ") with an error code of " + e.getRawStatusCode(), e);
            throw new KafkaDemoRetryableException(e);
        } catch (ResourceAccessException e) {
            log.error("Retryable Error calling thirdparty api, returned an (" + e.getClass().getName() + ")", e);
            throw new KafkaDemoRetryableException(e);
        } catch (Exception e) {
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ")", e);
            throw new KafkaDemoException(e);
        }
    }
}
