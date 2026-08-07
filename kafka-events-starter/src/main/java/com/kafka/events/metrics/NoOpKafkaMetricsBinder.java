package com.kafka.events.metrics;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

/**
 * Used when the application has no metrics registry.
 */
final class NoOpKafkaMetricsBinder implements KafkaMetricsBinder {

	static final NoOpKafkaMetricsBinder INSTANCE = new NoOpKafkaMetricsBinder();

	private static final AutoCloseable NOTHING_TO_CLOSE = () -> {
	};

	private NoOpKafkaMetricsBinder() {
	}

	@Override
	public AutoCloseable bindProducer(Producer<?, ?> producer, String clientId) {
		return NOTHING_TO_CLOSE;
	}

	@Override
	public AutoCloseable bindConsumer(Consumer<?, ?> consumer, String clientId) {
		return NOTHING_TO_CLOSE;
	}

}
