package com.windowforsun.kafka.transaction.integration;

import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.windowforsun.kafka.transaction.KafkaDemoConfig;
import com.windowforsun.kafka.transaction.event.DemoInboundEvent;
import com.windowforsun.kafka.transaction.util.TestEventData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.backoff.FixedBackOff;
import wiremock.org.eclipse.jetty.util.component.AbstractLifeCycle;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

@Slf4j
@SpringBootTest(classes = KafkaDemoConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true, count = 3, topics = {"demo-transactional-inbound-topic", "demo-non-transactional-inbound-topic"})
public class KafkaTransactionsIntegrationTest extends IntegrationTestBase {
    final static String DEMO_TRANSACTIONAL_TEST_TOPIC = "demo-transactional-inbound-topic";
    final static String DEMO_NON_TRANSACTIONAL_TEST_TOPIC = "demo-non-transactional-inbound-topic";

    @Autowired
    private KafkaTestListenerReadCommitted1 testReceiverReadCommitted1;
    @Autowired
    private KafkaTestListenerReadCommitted2 testReceiverReadCommitted2;
    @Autowired
    private KafkaTestListenerReadUnCommitted1 testReceiverReadUnCommitted1;
    @Autowired
    private KafkaTestListenerReadUnCommitted2 testReceiverReadUnCommitted2;

    @Configuration
    static class TestConfig {
        @Bean
        public KafkaTestListenerReadCommitted1 testReceiverReadCommitted1() {
            return new KafkaTestListenerReadCommitted1();
        }

        @Bean
        public KafkaTestListenerReadCommitted2 testReceiverReadCommitted2() {
            return new KafkaTestListenerReadCommitted2();
        }

        @Bean
        public KafkaTestListenerReadUnCommitted1 testReceiverReadUnCommitted1() {
            return new KafkaTestListenerReadUnCommitted1();
        }

        @Bean
        public KafkaTestListenerReadUnCommitted2 testReceiverReadUnCommitted2() {
            return new KafkaTestListenerReadUnCommitted2();
        }

        @Bean
        public ConsumerFactory<Object, Object> testConsumerFactoryReadCommitted(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaTransactionsIntegrationTest");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
            return new DefaultKafkaConsumerFactory<>(config);
        }

        @Bean
        public ConsumerFactory<Object, Object> testConsumerFactoryReadUncommitted(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaTransactionsIntegrationTest");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT));
            return new DefaultKafkaConsumerFactory<>(config);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerReadCommittedContainerFactory(final ConsumerFactory<Object, Object> testConsumerFactoryReadCommitted) {
            final SeekToCurrentErrorHandler errorHandler =
                    new SeekToCurrentErrorHandler((record, exception) -> {
                        // 4 seconds pause, 4 retries.
                    }, new FixedBackOff(4000L, 4L));
            final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory();
            factory.setConsumerFactory(testConsumerFactoryReadCommitted);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
            factory.setErrorHandler(errorHandler);
            return factory;
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerReadUnCommittedContainerFactory(final ConsumerFactory<Object, Object> testConsumerFactoryReadUncommitted) {
            final SeekToCurrentErrorHandler errorHandler =
                    new SeekToCurrentErrorHandler((record, exception) -> {
                        // 4 seconds pause, 4 retries.
                    }, new FixedBackOff(4000L, 4L));
            final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory();
            factory.setConsumerFactory(testConsumerFactoryReadUncommitted);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
            factory.setErrorHandler(errorHandler);
            return factory;
        }

        @Bean
        public ProducerFactory<String, String> testProducerFactory(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            log.debug("Test Kafka producer is created with brokerUrl={}", bootstrapServers);
            return new DefaultKafkaProducerFactory<>(config);
        }

        @Bean
        public KafkaTemplate<String, String> testKafkaTemplate(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            return new KafkaTemplate<>(testProducerFactory(bootstrapServers));
        }
    }

    public static class KafkaTestListenerReadCommitted1 {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaTransactionsIntegrationTestReadCommitted", topics = "demo-outbound-topic1", containerFactory = "kafkaListenerReadCommittedContainerFactory", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListener1 - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    public static class KafkaTestListenerReadCommitted2 {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaTransactionsIntegrationTestReadCommitted", topics = "demo-outbound-topic2", containerFactory = "kafkaListenerReadCommittedContainerFactory", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListener2 - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    public static class KafkaTestListenerReadUnCommitted1 {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaTransactionsIntegrationTestReadUnCommitted", topics = "demo-outbound-topic1", containerFactory = "kafkaListenerReadUnCommittedContainerFactory", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListenerReadUncommitted1 - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    public static class KafkaTestListenerReadUnCommitted2 {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaTransactionsIntegrationTestReadUnCommitted", topics = "demo-outbound-topic2", containerFactory = "kafkaListenerReadUnCommittedContainerFactory", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListenerReadUncommitted2 - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp() {
        super.setUp();
        this.testReceiverReadCommitted1.counter.set(0);
        this.testReceiverReadCommitted2.counter.set(0);
        this.testReceiverReadUnCommitted1.counter.set(0);
        this.testReceiverReadUnCommitted2.counter.set(0);
    }

    @Test
    public void testNonTransactionalSuccess() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        stubWiremock("/api/kafkatransactionsdemo/" + key, 200, "Success");

        sendMessage(DEMO_NON_TRANSACTIONAL_TEST_TOPIC, eventId, key, TestEventData.buildDemoInboundEvent(key));

        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadCommitted1.counter::get, equalTo(1));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadCommitted2.counter::get, equalTo(1));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadUnCommitted1.counter::get, equalTo(1));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadUnCommitted2.counter::get, equalTo(1));
        verify(exactly(1), getRequestedFor(urlEqualTo("/api/kafkatransactionsdemo/" + key)));
    }

    @Test
    public void testTransactionalSuccess() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        stubWiremock("/api/kafkatransactionsdemo/" + key, 200, "Success");

        sendMessage(DEMO_TRANSACTIONAL_TEST_TOPIC, eventId, key, TestEventData.buildDemoInboundEvent(key));

        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadCommitted1.counter::get, equalTo(1));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadCommitted2.counter::get, equalTo(1));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadUnCommitted1.counter::get, equalTo(1));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadUnCommitted2.counter::get, equalTo(1));
        verify(exactly(1), getRequestedFor(urlEqualTo("/api/kafkatransactionsdemo/" + key)));
    }

    @Test
    public void testExactlyOnce_NonTransactionalConsumer() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        stubWiremock("/api/kafkatransactionsdemo/" + key, 500, "Unavailable", "failOnce", Scenario.STARTED, "succeedNextTime");
        stubWiremock("/api/kafkatransactionsdemo/" + key, 200, "success", "failOnce", "succeedNextTime", "succeedNextTime");

        DemoInboundEvent inboundEvent = TestEventData.buildDemoInboundEvent(key);
        sendMessage(DEMO_NON_TRANSACTIONAL_TEST_TOPIC, eventId, key, inboundEvent);

        // check emitted demo-outbound-topic1
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadCommitted1.counter::get, equalTo(2));

        TimeUnit.SECONDS.sleep(5);
        assertThat(this.testReceiverReadCommitted1.counter.get(), is(2));
        assertThat(this.testReceiverReadCommitted2.counter.get(), is(1));
        assertThat(this.testReceiverReadUnCommitted1.counter.get(), is(2));
        assertThat(this.testReceiverReadUnCommitted2.counter.get(), is(1));
        verify(exactly(2), getRequestedFor(urlEqualTo("/api/kafkatransactionsdemo/" + key)));
    }

    @Test
    public void testExactlyOnce_TransactionalConsumer() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        stubWiremock("/api/kafkatransactionsdemo/" + key, 500, "Unavailable", "failOnce", Scenario.STARTED, "succeedNextTime");
        stubWiremock("/api/kafkatransactionsdemo/" + key, 200, "success", "failOnce", "succeedNextTime", "succeedNextTime");

        DemoInboundEvent inboundEvent = TestEventData.buildDemoInboundEvent(key);
        sendMessage(DEMO_TRANSACTIONAL_TEST_TOPIC, eventId, key, inboundEvent);

        // check emitted demo-outbound-topic1
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiverReadCommitted1.counter::get, equalTo(1));

        TimeUnit.SECONDS.sleep(5);
        assertThat(this.testReceiverReadCommitted1.counter.get(), is(1));
        assertThat(this.testReceiverReadCommitted2.counter.get(), is(1));

        assertThat(this.testReceiverReadUnCommitted1.counter.get(), is(2));
        assertThat(this.testReceiverReadUnCommitted2.counter.get(), is(1));
        verify(exactly(2), getRequestedFor(urlEqualTo("/api/kafkatransactionsdemo/" + key)));
    }
}
