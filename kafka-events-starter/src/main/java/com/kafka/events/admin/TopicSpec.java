package com.kafka.events.admin;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Declarative description of a topic this application depends on.
 *
 * <p>Bindable from configuration, so a service can declare the topics it owns next to the rest of
 * its config:
 *
 * <pre>
 * kafka:
 *   admin:
 *     topics:
 *       - name: orders
 *         partitions: 6
 *         configs:
 *           retention.ms: "604800000"
 * </pre>
 *
 * @param name topic name; must not be blank
 * @param partitions partition count; defaults to 3
 * @param replicationFactor replication factor, or {@code -1} to defer to the cluster default.
 *     Deferring is the right choice in production, where the cluster knows better than the app.
 * @param configs per-topic Kafka config overrides such as {@code retention.ms}
 */
public record TopicSpec(String name, @DefaultValue("3") int partitions,
		@DefaultValue("-1") short replicationFactor, @DefaultValue Map<String, String> configs) {

	public TopicSpec {
		if (name == null || name.isBlank()) {
			throw new IllegalArgumentException("Topic name must not be blank");
		}
		if (partitions <= 0) {
			throw new IllegalArgumentException("Topic '" + name + "' must have at least 1 partition, got " + partitions);
		}
		if (replicationFactor == 0 || replicationFactor < -1) {
			throw new IllegalArgumentException(
					"Topic '" + name + "' replication factor must be positive or -1 (cluster default), got "
							+ replicationFactor);
		}
		configs = (configs == null) ? Map.of() : Map.copyOf(configs);
	}

	/**
	 * A topic with the given name, 3 partitions and the cluster's default replication factor.
	 */
	public static TopicSpec of(String name) {
		return new TopicSpec(name, 3, (short) -1, Map.of());
	}

	public static TopicSpec of(String name, int partitions) {
		return new TopicSpec(name, partitions, (short) -1, Map.of());
	}

	/**
	 * Returns a copy of this spec renamed, keeping partition count and configs. Used to derive a
	 * dead-letter topic from the topic it shadows.
	 */
	public TopicSpec withName(String newName) {
		return new TopicSpec(newName, this.partitions, this.replicationFactor, this.configs);
	}

	public TopicSpec withConfig(String key, String value) {
		Map<String, String> merged = new LinkedHashMap<>(this.configs);
		merged.put(key, value);
		return new TopicSpec(this.name, this.partitions, this.replicationFactor, merged);
	}

	NewTopic toNewTopic() {
		Optional<Short> replication = (this.replicationFactor > 0) ? Optional.of(this.replicationFactor)
				: Optional.empty();
		NewTopic newTopic = new NewTopic(this.name, Optional.of(this.partitions), replication);
		return this.configs.isEmpty() ? newTopic : newTopic.configs(this.configs);
	}

}
