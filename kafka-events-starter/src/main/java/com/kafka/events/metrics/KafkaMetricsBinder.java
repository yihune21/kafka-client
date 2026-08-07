package com.kafka.events.metrics;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

/**
 * Registers a Kafka client's internal metrics with whatever monitoring the host application has.
 *
 * <p>This indirection exists so the runtime classes never mention Micrometer. A consumer app
 * without Micrometer on its classpath gets {@link #noop()} and loads cleanly; one with it gets
 * {@link MicrometerKafkaMetricsBinder} and the full set of client metrics, including consumer lag.
 */
public interface KafkaMetricsBinder {

	/**
	 * @return a handle that unregisters the metrics when the client is closed
	 */
	AutoCloseable bindProducer(Producer<?, ?> producer, String clientId);

	AutoCloseable bindConsumer(Consumer<?, ?> consumer, String clientId);

	static KafkaMetricsBinder noop() {
		return NoOpKafkaMetricsBinder.INSTANCE;
	}

}
