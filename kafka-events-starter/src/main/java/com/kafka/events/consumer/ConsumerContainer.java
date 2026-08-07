package com.kafka.events.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The running form of a {@link TopicSubscription}: a fixed pool of {@link ConsumerWorker}s, each
 * with its own consumer, all in the same consumer group.
 *
 * <p>Concurrency is bounded by partitions, not threads. Twelve workers on a three-partition topic
 * leaves nine of them permanently idle.
 */
public class ConsumerContainer {

	private static final Logger log = LoggerFactory.getLogger(ConsumerContainer.class);

	private final TopicSubscription subscription;

	private final int concurrency;

	private final Duration shutdownTimeout;

	private final IntFunction<ConsumerWorker> workerFactory;

	private final List<ConsumerWorker> workers = new ArrayList<>();

	private ExecutorService executor;

	private volatile boolean running;

	ConsumerContainer(TopicSubscription subscription, int concurrency, Duration shutdownTimeout,
			IntFunction<ConsumerWorker> workerFactory) {
		this.subscription = subscription;
		this.concurrency = concurrency;
		this.shutdownTimeout = shutdownTimeout;
		this.workerFactory = workerFactory;
	}

	public synchronized void start() {
		if (this.running) {
			return;
		}
		this.executor = Executors.newFixedThreadPool(this.concurrency, threadFactory());
		for (int index = 0; index < this.concurrency; index++) {
			ConsumerWorker worker = this.workerFactory.apply(index);
			this.workers.add(worker);
			this.executor.execute(worker);
		}
		this.running = true;
		log.info("Started consumer container '{}' with {} worker(s) on {}", this.subscription.id(), this.concurrency,
				this.subscription.describeTopics());
	}

	/**
	 * Asks every worker to finish its current record, commit and leave the group, then waits for
	 * them. Leaving the group cleanly is what stops a deploy from triggering a rebalance storm.
	 */
	public synchronized void stop() {
		if (!this.running) {
			return;
		}
		log.info("Stopping consumer container '{}'", this.subscription.id());
		this.workers.forEach(ConsumerWorker::stop);
		for (ConsumerWorker worker : this.workers) {
			try {
				if (!worker.awaitStop(this.shutdownTimeout)) {
					log.warn("Worker '{}' did not stop within {}", worker.name(), this.shutdownTimeout);
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		this.executor.shutdown();
		try {
			if (!this.executor.awaitTermination(this.shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
				this.executor.shutdownNow();
			}
		}
		catch (InterruptedException ex) {
			this.executor.shutdownNow();
			Thread.currentThread().interrupt();
		}
		this.workers.clear();
		this.executor = null;
		this.running = false;
		log.info("Stopped consumer container '{}'", this.subscription.id());
	}

	public boolean isRunning() {
		return this.running;
	}

	public String id() {
		return this.subscription.id();
	}

	public TopicSubscription subscription() {
		return this.subscription;
	}

	public int concurrency() {
		return this.concurrency;
	}

	/**
	 * Workers that exited on an unrecoverable error rather than on request. Any number above zero
	 * means partitions are going unconsumed.
	 */
	public long failedWorkers() {
		return this.workers.stream().filter(ConsumerWorker::isFailed).count();
	}

	public boolean isHealthy() {
		return this.running && failedWorkers() == 0;
	}

	private ThreadFactory threadFactory() {
		String prefix = "kafka-" + this.subscription.id() + "-";
		AtomicInteger counter = new AtomicInteger();
		return (runnable) -> {
			Thread thread = new Thread(runnable, prefix + counter.getAndIncrement());
			// Non-daemon on purpose: a consumer-only service has no other foreground thread, and
			// these are what keep the JVM alive until shutdown is actually requested.
			thread.setDaemon(false);
			return thread;
		};
	}

}
