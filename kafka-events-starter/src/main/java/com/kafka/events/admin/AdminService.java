package com.kafka.events.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.kafka.events.KafkaOperationException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Topic and cluster management, with every asynchronous admin call awaited.
 *
 * <p>The Kafka admin API returns futures that are easy to drop on the floor; a {@code createTopics}
 * whose result is never inspected reports success for a topic that was never created. Every method
 * here waits for the result and converts failure into a {@link KafkaOperationException}.
 *
 * <p>Instances are thread safe.
 */
public class AdminService implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(AdminService.class);

	private final Admin adminClient;

	private final Duration operationTimeout;

	private final AtomicBoolean closed = new AtomicBoolean();

	public AdminService(Admin adminClient, Duration operationTimeout) {
		this.adminClient = adminClient;
		this.operationTimeout = operationTimeout;
	}

	public static AdminService create(Map<String, Object> config, Duration operationTimeout) {
		return new AdminService(Admin.create(config), operationTimeout);
	}

	/**
	 * Creates the topic if it is absent, and does nothing if it is already there.
	 */
	public void ensureTopic(TopicSpec spec) {
		ensureTopics(List.of(spec));
	}

	/**
	 * Creates whichever of the given topics do not exist yet.
	 *
	 * <p>Safe to call from every instance of a service at once: losing the creation race produces
	 * {@link TopicExistsException}, which is the desired end state and so is not an error.
	 *
	 * <p>Existing topics are never modified. A topic whose live partition count differs from its
	 * declaration is reported as a warning rather than reshaped, because growing a topic changes
	 * which partition a key hashes to and silently breaks per-key ordering for every consumer.
	 */
	public void ensureTopics(Collection<TopicSpec> specs) {
		if (specs == null || specs.isEmpty()) {
			return;
		}
		Map<String, TopicSpec> byName = new LinkedHashMap<>();
		for (TopicSpec spec : specs) {
			TopicSpec existing = byName.putIfAbsent(spec.name(), spec);
			if (existing != null && !existing.equals(spec)) {
				throw new KafkaOperationException(
						"Topic '" + spec.name() + "' is declared twice with different settings");
			}
		}

		Set<String> alreadyPresent = listTopics();
		List<NewTopic> toCreate = byName.values()
			.stream()
			.filter((spec) -> !alreadyPresent.contains(spec.name()))
			.map(TopicSpec::toNewTopic)
			.toList();

		if (!toCreate.isEmpty()) {
			CreateTopicsResult result = this.adminClient.createTopics(toCreate);
			result.values().forEach(this::awaitCreation);
		}
		warnOnPartitionDrift(byName.values());
	}

	private void awaitCreation(String name, KafkaFuture<Void> future) {
		try {
			future.get(this.operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
			log.info("Created Kafka topic '{}'", name);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new KafkaOperationException("Interrupted while creating topic '" + name + "'", ex);
		}
		catch (ExecutionException ex) {
			if (ex.getCause() instanceof TopicExistsException) {
				log.debug("Topic '{}' already exists; another instance created it first", name);
				return;
			}
			throw new KafkaOperationException("Failed to create topic '" + name + "'", ex.getCause());
		}
		catch (TimeoutException ex) {
			throw new KafkaOperationException(
					"Timed out after " + this.operationTimeout + " creating topic '" + name + "'", ex);
		}
	}

	private void warnOnPartitionDrift(Collection<TopicSpec> specs) {
		Map<String, TopicDescription> descriptions = describeTopics(
				specs.stream().map(TopicSpec::name).collect(Collectors.toCollection(LinkedHashSet::new)));
		for (TopicSpec spec : specs) {
			TopicDescription description = descriptions.get(spec.name());
			if (description == null) {
				continue;
			}
			int actual = description.partitions().size();
			if (actual != spec.partitions()) {
				log.warn("Topic '{}' has {} partitions but is declared with {}. Leaving it as is: changing the "
						+ "partition count of a live topic remaps keys to different partitions and breaks per-key "
						+ "ordering. Adjust the declaration, or repartition deliberately.", spec.name(), actual,
						spec.partitions());
			}
		}
	}

	public boolean topicExists(String topicName) {
		return listTopics().contains(topicName);
	}

	public Set<String> listTopics() {
		return await(this.adminClient.listTopics().names(), "listing topics");
	}

	public TopicDescription describeTopic(String topicName) {
		TopicDescription description = describeTopics(Set.of(topicName)).get(topicName);
		if (description == null) {
			throw new KafkaOperationException("Topic '" + topicName + "' does not exist");
		}
		return description;
	}

	/**
	 * Describes the given topics, silently omitting any that do not exist.
	 */
	public Map<String, TopicDescription> describeTopics(Collection<String> topicNames) {
		if (topicNames.isEmpty()) {
			return Map.of();
		}
		Map<String, TopicDescription> descriptions = new LinkedHashMap<>();
		this.adminClient.describeTopics(topicNames).topicNameValues().forEach((name, future) -> {
			try {
				descriptions.put(name, future.get(this.operationTimeout.toMillis(), TimeUnit.MILLISECONDS));
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new KafkaOperationException("Interrupted while describing topic '" + name + "'", ex);
			}
			catch (ExecutionException ex) {
				if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
					return;
				}
				throw new KafkaOperationException("Failed to describe topic '" + name + "'", ex.getCause());
			}
			catch (TimeoutException ex) {
				throw new KafkaOperationException(
						"Timed out after " + this.operationTimeout + " describing topic '" + name + "'", ex);
			}
		});
		return descriptions;
	}

	/**
	 * Deletes a topic and every record in it. Returns quietly if the topic is already gone.
	 */
	public void deleteTopic(String topicName) {
		try {
			this.adminClient.deleteTopics(Set.of(topicName))
				.all()
				.get(this.operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
			log.info("Deleted Kafka topic '{}'", topicName);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new KafkaOperationException("Interrupted while deleting topic '" + topicName + "'", ex);
		}
		catch (ExecutionException ex) {
			if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
				log.debug("Topic '{}' was already absent", topicName);
				return;
			}
			throw new KafkaOperationException("Failed to delete topic '" + topicName + "'", ex.getCause());
		}
		catch (TimeoutException ex) {
			throw new KafkaOperationException(
					"Timed out after " + this.operationTimeout + " deleting topic '" + topicName + "'", ex);
		}
	}

	/**
	 * Records committed by the group but not yet consumed, per partition. The headline number for
	 * "is this consumer keeping up"; wire it into an alert.
	 *
	 * @return lag per partition, empty if the group has committed nothing
	 */
	public Map<TopicPartition, Long> consumerGroupLag(String groupId) {
		Map<TopicPartition, OffsetAndMetadata> committed = await(
				this.adminClient.listConsumerGroupOffsets(Map.of(groupId, new ListConsumerGroupOffsetsSpec()))
					.partitionsToOffsetAndMetadata(groupId),
				"listing committed offsets for group '" + groupId + "'");
		if (committed.isEmpty()) {
			return Map.of();
		}

		Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
		committed.keySet().forEach((partition) -> request.put(partition, OffsetSpec.latest()));
		Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = await(
				this.adminClient.listOffsets(request).all(), "listing end offsets for group '" + groupId + "'");

		Map<TopicPartition, Long> lag = new LinkedHashMap<>();
		committed.forEach((partition, offset) -> {
			ListOffsetsResult.ListOffsetsResultInfo end = endOffsets.get(partition);
			if (end != null && offset != null) {
				lag.put(partition, Math.max(0L, end.offset() - offset.offset()));
			}
		});
		return lag;
	}

	public ClusterInfo describeCluster() {
		var result = this.adminClient.describeCluster();
		String clusterId = await(result.clusterId(), "describing cluster");
		Collection<Node> nodes = await(result.nodes(), "listing cluster nodes");
		Node controller = await(result.controller(), "identifying cluster controller");
		String controllerId = (controller == null || controller.isEmpty()) ? "unknown" : controller.idString();
		return new ClusterInfo(clusterId, nodes.size(), controllerId);
	}

	/**
	 * The underlying client, for the admin operations this class does not wrap.
	 */
	public Admin rawAdminClient() {
		return this.adminClient;
	}

	@Override
	public void close() {
		if (this.closed.compareAndSet(false, true)) {
			this.adminClient.close(this.operationTimeout);
		}
	}

	private <T> T await(KafkaFuture<T> future, String description) {
		try {
			return future.get(this.operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new KafkaOperationException("Interrupted while " + description, ex);
		}
		catch (ExecutionException ex) {
			throw new KafkaOperationException("Failed " + description, ex.getCause());
		}
		catch (TimeoutException ex) {
			throw new KafkaOperationException("Timed out after " + this.operationTimeout + " while " + description, ex);
		}
	}

	/**
	 * Summary of the cluster the application is talking to.
	 */
	public record ClusterInfo(String clusterId, int nodeCount, String controllerId) {
	}

}
