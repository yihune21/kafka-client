package com.kafka.events.consumer;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * A declaration that this application consumes a set of topics with a given handler.
 *
 * <p>Publish one as a bean and the starter builds, starts and stops a container for it:
 *
 * <pre>
 * &#64;Bean
 * TopicSubscription orders(OrderHandler handler) {
 *     return TopicSubscription.builder()
 *         .topics("orders")
 *         .groupId("orders-service")
 *         .concurrency(3)
 *         .handler(handler)
 *         .build();
 * }
 * </pre>
 *
 * <p>Anything left unset falls back to the matching {@code kafka.consumer.*} property, so the
 * common case is a handler and a topic name.
 */
public final class TopicSubscription {

	private final String id;

	private final Set<String> topics;

	private final Pattern topicPattern;

	private final String groupId;

	private final MessageHandler handler;

	private final BatchMessageHandler batchHandler;

	private final Integer concurrency;

	private final RetryPolicy retryPolicy;

	private final FailureAction failureAction;

	private final String deadLetterTopic;

	private final Map<String, String> properties;

	private TopicSubscription(Builder builder) {
		this.topics = Set.copyOf(builder.topics);
		this.topicPattern = builder.topicPattern;
		this.groupId = builder.groupId;
		this.handler = builder.handler;
		this.batchHandler = builder.batchHandler;
		this.concurrency = builder.concurrency;
		this.retryPolicy = builder.retryPolicy;
		this.failureAction = builder.failureAction;
		this.deadLetterTopic = builder.deadLetterTopic;
		this.properties = Map.copyOf(builder.properties);
		this.id = (builder.id != null) ? builder.id : defaultId();
	}

	private String defaultId() {
		return (this.topicPattern != null) ? "pattern-" + this.topicPattern.pattern().replaceAll("\\W+", "-")
				: String.join("-", new TreeSet<>(this.topics));
	}

	public static Builder builder() {
		return new Builder();
	}

	public String id() {
		return this.id;
	}

	public Set<String> topics() {
		return this.topics;
	}

	public Pattern topicPattern() {
		return this.topicPattern;
	}

	/**
	 * @return the configured group, or null to inherit {@code kafka.consumer.group-id}
	 */
	public String groupId() {
		return this.groupId;
	}

	public MessageHandler handler() {
		return this.handler;
	}

	public BatchMessageHandler batchHandler() {
		return this.batchHandler;
	}

	public Integer concurrency() {
		return this.concurrency;
	}

	public RetryPolicy retryPolicy() {
		return this.retryPolicy;
	}

	public FailureAction failureAction() {
		return this.failureAction;
	}

	public String deadLetterTopic() {
		return this.deadLetterTopic;
	}

	public Map<String, String> properties() {
		return this.properties;
	}

	String describeTopics() {
		return (this.topicPattern != null) ? "pattern " + this.topicPattern.pattern() : this.topics.toString();
	}

	@Override
	public String toString() {
		return "TopicSubscription[" + this.id + " -> " + describeTopics() + "]";
	}

	public static final class Builder {

		private final Set<String> topics = new LinkedHashSet<>();

		private final Map<String, String> properties = new LinkedHashMap<>();

		private String id;

		private Pattern topicPattern;

		private String groupId;

		private MessageHandler handler;

		private BatchMessageHandler batchHandler;

		private Integer concurrency;

		private RetryPolicy retryPolicy;

		private FailureAction failureAction;

		private String deadLetterTopic;

		/**
		 * Identifies this subscription in logs, thread names and health details. Defaults to the
		 * topic names joined together.
		 */
		public Builder id(String id) {
			this.id = id;
			return this;
		}

		public Builder topics(String... topics) {
			this.topics.addAll(List.of(topics));
			return this;
		}

		public Builder topics(Iterable<String> topics) {
			topics.forEach(this.topics::add);
			return this;
		}

		/**
		 * Subscribes by regex instead of by name, picking up topics created after startup.
		 * Mutually exclusive with {@link #topics}.
		 */
		public Builder topicPattern(Pattern topicPattern) {
			this.topicPattern = topicPattern;
			return this;
		}

		/**
		 * Consumer group to join. Defaults to {@code kafka.consumer.group-id}.
		 */
		public Builder groupId(String groupId) {
			this.groupId = groupId;
			return this;
		}

		/**
		 * Per-record handler. Mutually exclusive with {@link #batchHandler}.
		 */
		public Builder handler(MessageHandler handler) {
			this.handler = handler;
			return this;
		}

		/**
		 * Whole-batch handler, for work that amortises well across records.
		 */
		public Builder batchHandler(BatchMessageHandler batchHandler) {
			this.batchHandler = batchHandler;
			return this;
		}

		/**
		 * Worker threads, each with its own consumer. Going beyond the topic's partition count
		 * leaves the extra workers idle.
		 */
		public Builder concurrency(int concurrency) {
			this.concurrency = concurrency;
			return this;
		}

		public Builder retryPolicy(RetryPolicy retryPolicy) {
			this.retryPolicy = retryPolicy;
			return this;
		}

		public Builder failureAction(FailureAction failureAction) {
			this.failureAction = failureAction;
			return this;
		}

		/**
		 * Explicit dead-letter topic. Defaults to the source topic plus
		 * {@code kafka.consumer.dead-letter.suffix}, which for a multi-topic subscription means
		 * each source topic gets its own.
		 */
		public Builder deadLetterTopic(String deadLetterTopic) {
			this.deadLetterTopic = deadLetterTopic;
			return this;
		}

		/**
		 * Raw Kafka consumer property applied to this subscription's workers only.
		 */
		public Builder property(String key, String value) {
			this.properties.put(key, value);
			return this;
		}

		public TopicSubscription build() {
			boolean hasNames = !this.topics.isEmpty();
			boolean hasPattern = this.topicPattern != null;
			if (hasNames == hasPattern) {
				throw new IllegalStateException("A subscription needs either topics or a topicPattern, not "
						+ (hasNames ? "both" : "neither"));
			}
			boolean hasHandler = this.handler != null;
			boolean hasBatchHandler = this.batchHandler != null;
			if (hasHandler == hasBatchHandler) {
				throw new IllegalStateException("A subscription needs either a handler or a batchHandler, not "
						+ (hasHandler ? "both" : "neither"));
			}
			if (this.concurrency != null && this.concurrency < 1) {
				throw new IllegalStateException("concurrency must be at least 1, got " + this.concurrency);
			}
			return new TopicSubscription(this);
		}

	}

}
