package com.kafka.events.producer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.kafka.events.KafkaOperationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishing side of the starter.
 *
 * <p>Kafka's own {@code send} reports failures through a callback that is trivially ignored, which
 * is how "we sent it" turns into "it never landed". Every method here surfaces the outcome: as a
 * {@link CompletableFuture} you can compose on, as a thrown exception, or at minimum as a logged
 * error.
 *
 * <p>Instances are thread safe and meant to be shared. One producer per application multiplexes
 * across every topic; creating one per request throws away batching and connection reuse.
 */
public class ProducerService implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

	private final Producer<String, String> producer;

	private final Duration closeTimeout;

	private final boolean transactional;

	private final AtomicBoolean closed = new AtomicBoolean();

	public ProducerService(Producer<String, String> producer, Duration closeTimeout, boolean transactional) {
		this.producer = producer;
		this.closeTimeout = closeTimeout;
		this.transactional = transactional;
		if (transactional) {
			this.producer.initTransactions();
		}
	}

	public static ProducerService create(Map<String, Object> config, Duration closeTimeout, boolean transactional) {
		return new ProducerService(new KafkaProducer<>(config), closeTimeout, transactional);
	}

	/**
	 * Publishes a record. Records sharing a key land on the same partition and are therefore
	 * delivered in order relative to each other; a null key round-robins.
	 *
	 * <p>The returned future completes on the producer's I/O thread. Do not block in a callback
	 * chained onto it, or you stall delivery for every other record in flight.
	 */
	public CompletableFuture<RecordMetadata> send(String topic, String key, String value) {
		return send(new ProducerRecord<>(topic, key, value));
	}

	public CompletableFuture<RecordMetadata> send(String topic, String key, String value,
			Map<String, String> headers) {
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
		headers.forEach((name, headerValue) -> record.headers()
			.add(name, (headerValue != null) ? headerValue.getBytes(StandardCharsets.UTF_8) : null));
		return send(record);
	}

	public CompletableFuture<RecordMetadata> send(ProducerRecord<String, String> record) {
		requireOpen();
		CompletableFuture<RecordMetadata> result = new CompletableFuture<>();
		try {
			this.producer.send(record, (metadata, exception) -> {
				if (exception != null) {
					result.completeExceptionally(exception);
				}
				else {
					result.complete(metadata);
				}
			});
		}
		catch (Exception ex) {
			// send() throws rather than calling back for serialization errors, an exhausted
			// buffer, and a fenced transactional producer. Funnel both paths into the future so
			// callers only have one place to handle failure.
			result.completeExceptionally(ex);
		}
		return result;
	}

	/**
	 * Publishes and blocks until the brokers acknowledge, bounded by
	 * {@code kafka.producer.delivery-timeout}. Use when the caller cannot proceed until the record
	 * is durable, such as before committing a database transaction that assumes it was sent.
	 */
	public RecordMetadata sendAndWait(String topic, String key, String value) {
		return join(send(topic, key, value), topic);
	}

	public RecordMetadata sendAndWait(ProducerRecord<String, String> record) {
		return join(send(record), record.topic());
	}

	/**
	 * Publishes without waiting, logging any failure. The record may still be lost, but never
	 * silently: use this only where losing one is acceptable.
	 */
	public void publish(String topic, String key, String value) {
		send(topic, key, value).whenComplete((metadata, failure) -> {
			if (failure != null) {
				log.error("Failed to publish to topic '{}' with key '{}'", topic, key, failure);
			}
		});
	}

	/**
	 * Blocks until every buffered record has been acknowledged or has failed.
	 */
	public void flush() {
		requireOpen();
		this.producer.flush();
	}

	/**
	 * Runs {@code work} inside a Kafka transaction, so that either all records it publishes become
	 * visible to {@code read_committed} consumers or none do.
	 *
	 * <p>Requires {@code kafka.producer.transactional-id} to be set.
	 */
	public void runInTransaction(Runnable work) {
		callInTransaction(() -> {
			work.run();
			return null;
		});
	}

	public <T> T callInTransaction(Supplier<T> work) {
		requireOpen();
		if (!this.transactional) {
			throw new IllegalStateException(
					"Transactions require kafka.producer.transactional-id to be configured");
		}
		this.producer.beginTransaction();
		try {
			T result = work.get();
			this.producer.commitTransaction();
			return result;
		}
		catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException ex) {
			// Fatal by contract: this producer's epoch is gone, so aborting would fail too. The
			// only correct move is to close and let the instance be replaced.
			close();
			throw new KafkaOperationException(
					"Producer was fenced or is no longer authorised; transaction abandoned and producer closed", ex);
		}
		catch (RuntimeException ex) {
			try {
				this.producer.abortTransaction();
			}
			catch (RuntimeException abortFailure) {
				ex.addSuppressed(abortFailure);
			}
			throw ex;
		}
	}

	/**
	 * The underlying client, for the rare call this class does not wrap. Do not close it directly.
	 */
	public Producer<String, String> rawProducer() {
		return this.producer;
	}

	/**
	 * Flushes and closes. Called automatically when the application context shuts down, after
	 * consumer containers have stopped so that in-flight dead-letter writes still have a producer.
	 */
	@Override
	public void close() {
		if (this.closed.compareAndSet(false, true)) {
			log.info("Closing Kafka producer, waiting up to {} for in-flight records", this.closeTimeout);
			this.producer.close(this.closeTimeout);
		}
	}

	private RecordMetadata join(CompletableFuture<RecordMetadata> future, String topic) {
		try {
			return future.join();
		}
		catch (CompletionException ex) {
			Throwable cause = (ex.getCause() != null) ? ex.getCause() : ex;
			throw new KafkaOperationException("Failed to publish to topic '" + topic + "'", cause);
		}
	}

	private void requireOpen() {
		if (this.closed.get()) {
			throw new IllegalStateException("Producer has been closed");
		}
	}

}
