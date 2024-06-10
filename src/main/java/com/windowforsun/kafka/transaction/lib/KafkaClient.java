package com.windowforsun.kafka.transaction.lib;

import com.windowforsun.kafka.transaction.exception.KafkaDemoException;
import com.windowforsun.kafka.transaction.properties.KafkaDemoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaClient {
    private final KafkaDemoProperties properties;
    private final KafkaTemplate kafkaTemplateTransactional;
    private final KafkaTemplate kafkaTemplateNonTransactional;
    public static final String EVENT_ID_HEADER_KEY = "demo_eventIdHeader";

    public SendResult sendMessageWithTransaction(String key, String data, String outboundTopic) {
        return this.sendMessage("Transactional", this.kafkaTemplateTransactional, key, data, outboundTopic);
    }

    public SendResult sendMessageWithoutTransaction(String key, String data, String outboundTopic) {
        return this.sendMessage("NonTransactional", this.kafkaTemplateNonTransactional, key, data, outboundTopic);
    }

    public SendResult sendMessage(String type, KafkaTemplate kafkaTemplate, String key, String data, String outboundTopic) {
        try {
            String payload = "eventId:" + UUID.randomUUID() + ", instanceId:" + this.properties.getInstanceID() + ". payload:" + data;
            final ProducerRecord<String, String> record = new ProducerRecord<>(outboundTopic, key, payload);
            final SendResult result = (SendResult) kafkaTemplate.send(record).get();
            final RecordMetadata metadata = result.getRecordMetadata();

            log.debug("Send {} record(key={} value={}) meta(topic={}, partition={}, offset={}",
                    type, record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset());

            return result;
        } catch (Exception e) {
            log.error("Error sending message to topic : " + outboundTopic, e);
            throw new KafkaDemoException(e);
        }
    }
}
