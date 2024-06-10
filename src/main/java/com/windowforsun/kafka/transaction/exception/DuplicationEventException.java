package com.windowforsun.kafka.transaction.exception;

import java.util.UUID;

public class DuplicationEventException extends RuntimeException{
    public DuplicationEventException(final UUID eventId) {
        super("Duplicate event Id: " + eventId.toString());
    }
}
