package com.kafka.events.consumer;

import java.time.Duration;
import java.util.Map;
import java.util.function.IntFunction;

import com.kafka.events.config.KafkaClientConfigFactory;
import com.kafka.events.config.KafkaProperties;
import com.kafka.events.metrics.KafkaMetricsBinder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.springframework.util.StringUtils;

/**
 * Builds a running {@link ConsumerContainer} from a declared {@link TopicSubscription}, filling in
 * whatever the subscription left unset from {@code kafka.consumer.*}.
 */
public class ConsumerContainerFactory {

	/**
	 * Grace added to the consumer close timeout when waiting for a worker to finish, so the worker
	 * gets a chance to close its consumer cleanly before the container gives up on it.
	 */
	private static final Duration SHUTDOWN_GRACE = Duration.ofSeconds(5);

	private final KafkaProperties properties;

	private final KafkaClientConfigFactory configFactory;

	private final DeadLetterPublisher deadLetterPublisher;

	private final KafkaMetricsBinder metricsBinder;

	public ConsumerContainerFactory(KafkaProperties properties, KafkaClientConfigFactory configFactory,
			DeadLetterPublisher deadLetterPublisher, KafkaMetricsBinder metricsBinder) {
		this.properties = properties;
		this.configFactory = configFactory;
		this.deadLetterPublisher = deadLetterPublisher;
		this.metricsBinder = metricsBinder;
	}

	public ConsumerContainer create(TopicSubscription subscription) {
		KafkaProperties.Consumer consumerProperties = this.properties.getConsumer();
		String groupId = resolveGroupId(subscription);
		int concurrency = (subscription.concurrency() != null) ? subscription.concurrency()
				: consumerProperties.getConcurrency();
		RetryPolicy retryPolicy = (subscription.retryPolicy() != null) ? subscription.retryPolicy()
				: toRetryPolicy(consumerProperties.getRetry());
		FailureAction failureAction = (subscription.failureAction() != null) ? subscription.failureAction()
				: consumerProperties.getOnFailure();

		if (failureAction == FailureAction.DEAD_LETTER && this.deadLetterPublisher == null) {
			throw new IllegalStateException("Subscription '" + subscription.id()
					+ "' dead-letters failed records but no producer is available. Either leave kafka.producer "
					+ "enabled or set the failure action to SKIP or STOP.");
		}

		IntFunction<ConsumerWorker> workerFactory = (index) -> {
			String clientId = this.configFactory.clientId("consumer-" + subscription.id(), index);
			Map<String, Object> config = this.configFactory.consumerConfig(groupId, clientId,
					subscription.properties());
			Consumer<String, String> consumer = new KafkaConsumer<>(config);
			AutoCloseable metricsHandle = this.metricsBinder.bindConsumer(consumer, clientId);
			return new ConsumerWorker(subscription.id() + "-" + index, subscription, consumer, groupId,
					consumerProperties.getPollTimeout(), consumerProperties.getCloseTimeout(), retryPolicy,
					failureAction, this.deadLetterPublisher, metricsHandle);
		};

		Duration shutdownTimeout = consumerProperties.getCloseTimeout().plus(SHUTDOWN_GRACE);
		return new ConsumerContainer(subscription, concurrency, shutdownTimeout, workerFactory);
	}

	private String resolveGroupId(TopicSubscription subscription) {
		if (StringUtils.hasText(subscription.groupId())) {
			return subscription.groupId();
		}
		if (StringUtils.hasText(this.properties.getConsumer().getGroupId())) {
			return this.properties.getConsumer().getGroupId();
		}
		throw new IllegalStateException("Subscription '" + subscription.id()
				+ "' has no consumer group. Set kafka.consumer.group-id, or groupId() on the subscription.");
	}

	private static RetryPolicy toRetryPolicy(KafkaProperties.Retry retry) {
		return new RetryPolicy(retry.getMaxAttempts(), retry.getInitialBackoff(), retry.getMultiplier(),
				retry.getMaxBackoff());
	}

}
