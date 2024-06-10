package com.windowforsun.kafka.transaction.integration;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.windowforsun.kafka.transaction.lib.KafkaClient;
import com.windowforsun.kafka.transaction.KafkaDemoConfig;
import com.windowforsun.kafka.transaction.event.DemoInboundEvent;
import com.windowforsun.kafka.transaction.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

@Slf4j
@SpringBootTest(classes = KafkaDemoConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureWireMock(port = 0)
@ActiveProfiles("test")
public class IntegrationTestBase {
    @Autowired
    private KafkaTemplate<String, String> testKafkaTemplate;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    void setUp() {
        this.resetWiremock();

        this.registry.getListenerContainers()
                .stream()
                .forEach(container -> {
                    ContainerTestUtils.waitForAssignment(container,embeddedKafkaBroker.getPartitionsPerTopic());
                });
    }

    void resetWiremock() {
        log.debug("**** resetting Wiremock ****");
        WireMock.reset();
        WireMock.resetAllRequests();
        WireMock.resetAllScenarios();
        WireMock.resetToDefault();
    }

    void stubWiremock(String url, int httpStatusResponse, String body) {
        stubWiremock(url, httpStatusResponse, body, null, null, null);
    }

    void stubWiremock(String url, int httpStatusResponse, String body, String scenario, String initialState, String nextState) {
        if (scenario != null) {
            stubFor(get(urlEqualTo(url))
                    .inScenario(scenario)
                    .whenScenarioStateIs(initialState)
                    .willReturn(aResponse().withStatus(httpStatusResponse).withHeader("Content-Type", "text/plain").withBody(body))
                    .willSetStateTo(nextState));
        } else {
            stubFor(get(urlEqualTo(url))
                    .willReturn(aResponse().withStatus(httpStatusResponse).withHeader("Content-Type", "text/plain").withBody(body)));
        }
    }

    SendResult sendMessage(String topic, String eventId, String key, DemoInboundEvent event) throws Exception {
        String payload = JsonMapper.writeToJson(event);
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader(KafkaClient.EVENT_ID_HEADER_KEY, eventId != null ? eventId.getBytes() : null));
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, key, payload, headers);

        final SendResult result = (SendResult) this.testKafkaTemplate.send(record).get();
        final RecordMetadata metadata = result.getRecordMetadata();

        log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

        return result;

    }
}
