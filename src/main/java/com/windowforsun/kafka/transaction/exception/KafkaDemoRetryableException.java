package com.windowforsun.kafka.transaction.exception;

public class KafkaDemoRetryableException extends RuntimeException implements Retryable{
    public KafkaDemoRetryableException(Throwable cause) {
        super(cause);
    }
}
