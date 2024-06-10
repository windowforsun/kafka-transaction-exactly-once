package com.windowforsun.kafka.transaction.util;

import com.windowforsun.kafka.transaction.event.DemoInboundEvent;

public class TestEventData {
    public static String INBOUND_DATA = "event data";

    public static DemoInboundEvent buildDemoInboundEvent(String id) {
        return DemoInboundEvent.builder()
                .id(id)
                .data(INBOUND_DATA)
                .build();
    }
}
