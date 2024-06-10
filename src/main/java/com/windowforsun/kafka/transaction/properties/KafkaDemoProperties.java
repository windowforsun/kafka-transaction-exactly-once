package com.windowforsun.kafka.transaction.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import java.net.URL;
import java.util.UUID;

@Data
@Validated
@Configuration
@ConfigurationProperties("kafkademo")
public class KafkaDemoProperties {
    @NotNull
    private String id;
    @NotNull
    private URL thirdpartyEndpoint;
    @NotNull
    private String outboundTopic1;
    @NotNull
    private String outboundTopic2;
    @NotNull
    private UUID instanceID = UUID.randomUUID();
}
