package com.kafka.events.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One polling thread bound to one Kafka consumer.
 *
 * <p>A Kafka consumer is not thread safe, so this class owns its consumer outright: every call
 * except {@link #stop()} happens on the worker's own thread. {@code stop()} uses
 * {@code wakeup()}, the single method the client documents as safe to call from elsewhere.
 *
 * <p>Delivery is at-least-once. Offsets are committed only after a handler returns normally, so a
 * worker that dies mid-batch causes redelivery rather than loss, and handlers must be idempotent.
 */
class ConsumerWorker implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);

	/**
	 * How long a single keep-alive poll blocks while a handler is backing off between retries.
	 */
	private static final Duration KEEP_ALIVE_POLL = Duration.ofMillis(200);

	private final String name;

	private final TopicSubscription subscription;

	private final Consumer<String, String> consumer;

	private final String groupId;

	private final Duration pollTimeout;

	private final Duration closeTimeout;

	private final RetryPolicy retryPolicy;

	private final FailureAction failureAction;

	private final DeadLetterPublisher deadLetterPublisher;

	private final AutoCloseable metricsHandle;

	private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

	private final AtomicBoolean running = new AtomicBoolean(true);

	private final CountDownLatch stopped = new CountDownLatch(1);

	private volatile boolean failed;

	ConsumerWorker(String name, TopicSubscription subscription, Consumer<String, String> consumer, String groupId,
			Duration pollTimeout, Duration closeTimeout, RetryPolicy retryPolicy, FailureAction failureAction,
			DeadLetterPublisher deadLetterPublisher, AutoCloseable metricsHandle) {
		this.name = name;
		this.subscription = subscription;
		this.consumer = consumer;
		this.groupId = groupId;
		this.pollTimeout = pollTimeout;
		this.closeTimeout = closeTimeout;
		this.retryPolicy = retryPolicy;
		this.failureAction = failureAction;
		this.deadLetterPublisher = deadLetterPublisher;
		this.metricsHandle = metricsHandle;
	}

	@Override
	public void run() {
		log.info("[{}] starting, group '{}', subscribing to {}", this.name, this.groupId,
				this.subscription.describeTopics());
		try {
			subscribe();
			while (this.running.get()) {
				ConsumerRecords<String, String> records = this.consumer.poll(this.pollTimeout);
				if (records.isEmpty()) {
					continue;
				}
				if (this.subscription.batchHandler() != null) {
					processBatch(records);
				}
				else {
					processIndividually(records);
				}
				commitPending();
			}
		}
		catch (WakeupException ex) {
			if (this.running.get()) {
				this.failed = true;
				log.error("[{}] woken up without a shutdown request", this.name, ex);
			}
		}
		catch (Exception ex) {
			this.failed = true;
			log.error("[{}] stopping after an unrecoverable error; uncommitted records will be redelivered",
					this.name, ex);
		}
		finally {
			shutdown();
		}
	}

	private void subscribe() {
		ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				// Last chance to commit before these partitions move to another member. Anything
				// still pending here is reprocessed by whoever picks them up.
				log.debug("[{}] revoking {}", ConsumerWorker.this.name, partitions);
				commitPending();
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				log.info("[{}] assigned {}", ConsumerWorker.this.name, partitions);
			}

			@Override
			public void onPartitionsLost(Collection<TopicPartition> partitions) {
				// Already reassigned elsewhere; a commit would be rejected, so drop the offsets.
				partitions.forEach(ConsumerWorker.this.pendingOffsets::remove);
				log.warn("[{}] lost {} without a clean revocation; those records will be reprocessed",
						ConsumerWorker.this.name, partitions);
			}

		};

		if (this.subscription.topicPattern() != null) {
			this.consumer.subscribe(this.subscription.topicPattern(), listener);
		}
		else {
			this.consumer.subscribe(this.subscription.topics(), listener);
		}
	}

	private void processIndividually(ConsumerRecords<String, String> records) {
		for (ConsumerRecord<String, String> record : records) {
			if (!this.running.get()) {
				// Leave the rest of the batch uncommitted; it is redelivered on restart.
				return;
			}
			if (!handleWithRetry(record)) {
				return;
			}
			markConsumed(record);
		}
	}

	/**
	 * @return true if processing should continue, false if the worker is stopping
	 */
	private boolean handleWithRetry(ConsumerRecord<String, String> record) {
		int attempt = 1;
		while (true) {
			try {
				this.subscription.handler().handle(record);
				return true;
			}
			catch (Exception ex) {
				restoreInterruptFlag(ex);
				if (attempt >= this.retryPolicy.maxAttempts()) {
					return applyFailureAction(List.of(record), ex, attempt);
				}
				log.warn("[{}] handler failed for {}-{}@{} on attempt {}/{}, retrying", this.name, record.topic(),
						record.partition(), record.offset(), attempt, this.retryPolicy.maxAttempts(), ex);
				backoff(this.retryPolicy.backoffAfter(attempt));
				if (!this.running.get()) {
					return false;
				}
				attempt++;
			}
		}
	}

	private void processBatch(ConsumerRecords<String, String> records) {
		List<ConsumerRecord<String, String>> batch = new ArrayList<>();
		records.forEach(batch::add);

		int attempt = 1;
		while (true) {
			try {
				this.subscription.batchHandler().handle(batch);
				batch.forEach(this::markConsumed);
				return;
			}
			catch (Exception ex) {
				restoreInterruptFlag(ex);
				if (attempt >= this.retryPolicy.maxAttempts()) {
					if (applyFailureAction(batch, ex, attempt)) {
						batch.forEach(this::markConsumed);
					}
					return;
				}
				log.warn("[{}] batch handler failed for {} record(s) on attempt {}/{}, retrying", this.name,
						batch.size(), attempt, this.retryPolicy.maxAttempts(), ex);
				backoff(this.retryPolicy.backoffAfter(attempt));
				if (!this.running.get()) {
					return;
				}
				attempt++;
			}
		}
	}

	/**
	 * @return true if the records may now be committed past, false if the worker is stopping and
	 *     they must stay uncommitted for redelivery
	 */
	private boolean applyFailureAction(List<ConsumerRecord<String, String>> records, Exception failure, int attempts) {
		ConsumerRecord<String, String> first = records.get(0);
		switch (this.failureAction) {
			case DEAD_LETTER -> {
				if (this.deadLetterPublisher == null) {
					log.error("[{}] dead-lettering is configured but no producer is available; stopping rather "
							+ "than dropping {} record(s). Enable kafka.producer or pick another on-failure action.",
							this.name, records.size());
					this.running.set(false);
					this.failed = true;
					return false;
				}
				// Throws if the dead letter cannot be written, which propagates out of the poll
				// loop and leaves the source offset uncommitted. Failing loudly beats losing data.
				for (ConsumerRecord<String, String> record : records) {
					this.deadLetterPublisher.publish(record, deadLetterTopicFor(record), this.groupId, failure,
							attempts);
				}
				log.error("[{}] gave up on {} record(s) starting at {}-{}@{} after {} attempt(s); routed to '{}'",
						this.name, records.size(), first.topic(), first.partition(), first.offset(), attempts,
						deadLetterTopicFor(first), failure);
				return true;
			}
			case SKIP -> {
				log.error("[{}] discarding {} record(s) starting at {}-{}@{} after {} attempt(s)", this.name,
						records.size(), first.topic(), first.partition(), first.offset(), attempts, failure);
				return true;
			}
			case STOP -> {
				log.error("[{}] stopping at {}-{}@{} after {} failed attempt(s); the record stays uncommitted and "
						+ "is redelivered on restart", this.name, first.topic(), first.partition(), first.offset(),
						attempts, failure);
				this.running.set(false);
				this.failed = true;
				return false;
			}
		}
		return false;
	}

	private String deadLetterTopicFor(ConsumerRecord<String, String> record) {
		String configured = this.subscription.deadLetterTopic();
		return (configured != null) ? configured : this.deadLetterPublisher.deadLetterTopicFor(record.topic());
	}

	/**
	 * Waits out a retry backoff without letting the broker conclude this consumer is dead.
	 *
	 * <p>Sleeping outright would stall {@code poll()}, and once the gap between polls exceeds
	 * {@code max.poll.interval.ms} the group evicts this member and rebalances its partitions
	 * away, typically right as every other worker is backing off too. Pausing the assignment and
	 * continuing to poll keeps the membership alive while returning no records.
	 */
	private void backoff(Duration delay) {
		if (delay.isZero() || delay.isNegative()) {
			return;
		}
		Set<TopicPartition> paused = new HashSet<>(this.consumer.assignment());
		this.consumer.pause(paused);
		try {
			long deadline = System.nanoTime() + delay.toNanos();
			while (this.running.get()) {
				long remaining = deadline - System.nanoTime();
				if (remaining <= 0) {
					break;
				}
				Duration slice = Duration.ofNanos(Math.min(remaining, KEEP_ALIVE_POLL.toNanos()));
				ConsumerRecords<String, String> unexpected = this.consumer.poll(slice);
				if (!unexpected.isEmpty()) {
					// A rebalance mid-backoff assigned partitions that were not in the paused set.
					// Pause them and rewind, so these records come back through the normal path
					// instead of being skipped with their offsets silently advanced past.
					Set<TopicPartition> fresh = unexpected.partitions();
					this.consumer.pause(fresh);
					paused.addAll(fresh);
					for (TopicPartition partition : fresh) {
						this.consumer.seek(partition, unexpected.records(partition).get(0).offset());
					}
					log.debug("[{}] rewound {} partition(s) assigned during backoff", this.name, fresh.size());
				}
			}
		}
		finally {
			Set<TopicPartition> stillAssigned = new HashSet<>(this.consumer.assignment());
			stillAssigned.retainAll(paused);
			this.consumer.resume(stillAssigned);
		}
	}

	private void markConsumed(ConsumerRecord<String, String> record) {
		// Kafka commits the offset of the *next* record to read, hence the +1.
		this.pendingOffsets.put(new TopicPartition(record.topic(), record.partition()),
				new OffsetAndMetadata(record.offset() + 1));
	}

	private void commitPending() {
		if (this.pendingOffsets.isEmpty()) {
			return;
		}
		Map<TopicPartition, OffsetAndMetadata> toCommit = Map.copyOf(this.pendingOffsets);
		try {
			this.consumer.commitSync(toCommit);
			this.pendingOffsets.clear();
		}
		catch (CommitFailedException | RebalanceInProgressException ex) {
			// The group rebalanced while these records were being processed. They belong to
			// another member now and will be redelivered there, so discard rather than retry.
			log.warn("[{}] commit rejected after a rebalance; {} partition(s) will be reprocessed elsewhere",
					this.name, toCommit.size(), ex);
			this.pendingOffsets.clear();
		}
	}

	private void shutdown() {
		try {
			commitPending();
		}
		catch (Exception ex) {
			log.warn("[{}] final commit failed; some records will be reprocessed", this.name, ex);
		}
		try {
			this.consumer.close(this.closeTimeout);
		}
		catch (Exception ex) {
			log.warn("[{}] error closing consumer", this.name, ex);
		}
		if (this.metricsHandle != null) {
			try {
				this.metricsHandle.close();
			}
			catch (Exception ex) {
				log.debug("[{}] error unbinding metrics", this.name, ex);
			}
		}
		this.stopped.countDown();
		log.info("[{}] stopped", this.name);
	}

	/**
	 * Signals the worker to finish its current record and exit. Safe to call from any thread.
	 */
	void stop() {
		this.running.set(false);
		this.consumer.wakeup();
	}

	boolean awaitStop(Duration timeout) throws InterruptedException {
		return this.stopped.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	boolean isFailed() {
		return this.failed;
	}

	String name() {
		return this.name;
	}

	private static void restoreInterruptFlag(Exception ex) {
		if (ex instanceof InterruptedException) {
			Thread.currentThread().interrupt();
		}
	}

}
