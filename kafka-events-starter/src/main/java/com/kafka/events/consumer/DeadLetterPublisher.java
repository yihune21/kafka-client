package com.kafka.events.consumer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import com.kafka.events.producer.ProducerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Moves a record that no amount of retrying could process onto a dead-letter topic, together with
 * enough context to work out what went wrong.
 *
 * <p>The write is synchronous by design. Committing the source offset before the dead letter is
 * acknowledged would turn a processing failure into silent data loss, which is exactly the outcome
 * a dead-letter topic exists to prevent.
 */
public class DeadLetterPublisher {

	public static final String ORIGINAL_TOPIC = "kafka.dlt.original-topic";

	public static final String ORIGINAL_PARTITION = "kafka.dlt.original-partition";

	public static final String ORIGINAL_OFFSET = "kafka.dlt.original-offset";

	public static final String ORIGINAL_TIMESTAMP = "kafka.dlt.original-timestamp";

	public static final String GROUP_ID = "kafka.dlt.group-id";

	public static final String ATTEMPTS = "kafka.dlt.attempts";

	public static final String EXCEPTION_CLASS = "kafka.dlt.exception-class";

	public static final String EXCEPTION_MESSAGE = "kafka.dlt.exception-message";

	public static final String EXCEPTION_STACKTRACE = "kafka.dlt.exception-stacktrace";

	/**
	 * Stack traces are truncated so a pathological one cannot push the record past the broker's
	 * max message size and fail the very write that was meant to preserve it.
	 */
	private static final int MAX_STACKTRACE_CHARS = 4096;

	private final ProducerService producer;

	private final String suffix;

	public DeadLetterPublisher(ProducerService producer, String suffix) {
		this.producer = producer;
		this.suffix = suffix;
	}

	public String deadLetterTopicFor(String sourceTopic) {
		return sourceTopic + this.suffix;
	}

	/**
	 * Publishes the record to {@code deadLetterTopic} and blocks until the brokers acknowledge it.
	 *
	 * @throws com.kafka.events.KafkaOperationException if the dead letter could not be written, in
	 *     which case the caller must not commit the source offset
	 */
	public void publish(ConsumerRecord<String, String> record, String deadLetterTopic, String groupId,
			Throwable failure, int attempts) {
		ProducerRecord<String, String> deadLetter = new ProducerRecord<>(deadLetterTopic, record.key(),
				record.value());

		// Carry the original headers through first, so anything the producer attached (trace ids,
		// schema versions) survives, then layer the failure metadata on top.
		record.headers().forEach((header) -> deadLetter.headers().add(header));

		addHeader(deadLetter, ORIGINAL_TOPIC, record.topic());
		addHeader(deadLetter, ORIGINAL_PARTITION, Integer.toString(record.partition()));
		addHeader(deadLetter, ORIGINAL_OFFSET, Long.toString(record.offset()));
		addHeader(deadLetter, ORIGINAL_TIMESTAMP, Long.toString(record.timestamp()));
		addHeader(deadLetter, GROUP_ID, groupId);
		addHeader(deadLetter, ATTEMPTS, Integer.toString(attempts));
		addHeader(deadLetter, EXCEPTION_CLASS, failure.getClass().getName());
		addHeader(deadLetter, EXCEPTION_MESSAGE, failure.getMessage());
		addHeader(deadLetter, EXCEPTION_STACKTRACE, stackTraceOf(failure));

		this.producer.sendAndWait(deadLetter);
	}

	private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
		if (value != null) {
			record.headers().remove(name);
			record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
		}
	}

	private static String stackTraceOf(Throwable failure) {
		StringWriter writer = new StringWriter();
		failure.printStackTrace(new PrintWriter(writer));
		String trace = writer.toString();
		return (trace.length() <= MAX_STACKTRACE_CHARS) ? trace
				: trace.substring(0, MAX_STACKTRACE_CHARS) + "... [truncated]";
	}

}
