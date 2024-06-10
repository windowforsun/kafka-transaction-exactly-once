package com.windowforsun.kafka.transaction.consumer;

import com.windowforsun.kafka.transaction.lib.KafkaClient;
import com.windowforsun.kafka.transaction.event.DemoInboundEvent;
import com.windowforsun.kafka.transaction.exception.DuplicationEventException;
import com.windowforsun.kafka.transaction.exception.Retryable;
import com.windowforsun.kafka.transaction.mapper.JsonMapper;
import com.windowforsun.kafka.transaction.service.DemoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaTransactionalConsumer {
    private final AtomicInteger counter = new AtomicInteger();
    private final DemoService demoService;

    @KafkaListener(topics = "demo-transactional-inbound-topic", groupId = "kafkaConsumerGroup", containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Header(KafkaClient.EVENT_ID_HEADER_KEY) String eventId, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key, @Payload final String payload) {
        counter.getAndIncrement();
        log.debug("Received message [" + counter.get() + "] - eventId: " + eventId + " - key: " + key + " - payload: " + payload);

        try {
            DemoInboundEvent event = JsonMapper.readFromJson(payload, DemoInboundEvent.class);
            this.demoService.processWithTransaction(key, event);
        } catch (DuplicationEventException e) {
            // Update consumer offsets to ensure event is not again redelivered.
            log.debug("Duplicate message received: "+ e.getMessage());
        } catch (Exception e) {
            if (e instanceof Retryable) {
                log.debug("Throwing retryable exception.");
                throw e;
            }
            log.error("Error processing message: " + e.getMessage());
        }
    }
}
