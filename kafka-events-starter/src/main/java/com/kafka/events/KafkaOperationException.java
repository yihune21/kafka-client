package com.kafka.events;

/**
 * Thrown when a Kafka operation fails in a way the caller cannot be expected to recover from
 * in place, such as a topic that could not be created or an admin call that timed out.
 *
 * <p>Checked Kafka failures and {@link InterruptedException} are wrapped in this type so callers
 * are not forced to unwrap {@link java.util.concurrent.ExecutionException} at every call site.
 */
public class KafkaOperationException extends RuntimeException {

	public KafkaOperationException(String message) {
		super(message);
	}

	public KafkaOperationException(String message, Throwable cause) {
		super(message, cause);
	}

}
