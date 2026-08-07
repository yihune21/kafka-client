package com.kafka.events.metrics;

import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

/**
 * Publishes the Kafka clients' own metrics through Micrometer, which is where
 * {@code kafka_consumer_fetch_manager_records_lag_max} and friends come from.
 *
 * <p>Only instantiated when Micrometer is on the classpath; see {@link KafkaMetricsBinder}.
 */
public class MicrometerKafkaMetricsBinder implements KafkaMetricsBinder {

	private final MeterRegistry registry;

	public MicrometerKafkaMetricsBinder(MeterRegistry registry) {
		this.registry = registry;
	}

	@Override
	public AutoCloseable bindProducer(Producer<?, ?> producer, String clientId) {
		KafkaClientMetrics metrics = new KafkaClientMetrics(producer, tags(clientId, "producer"));
		metrics.bindTo(this.registry);
		return metrics;
	}

	@Override
	public AutoCloseable bindConsumer(Consumer<?, ?> consumer, String clientId) {
		KafkaClientMetrics metrics = new KafkaClientMetrics(consumer, tags(clientId, "consumer"));
		metrics.bindTo(this.registry);
		return metrics;
	}

	private static List<Tag> tags(String clientId, String role) {
		return List.of(Tag.of("kafka.client.id", clientId), Tag.of("kafka.client.role", role));
	}

}
