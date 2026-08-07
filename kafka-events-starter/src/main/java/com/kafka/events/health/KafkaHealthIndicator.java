package com.kafka.events.health;

import java.util.LinkedHashMap;
import java.util.Map;

import com.kafka.events.admin.AdminService;
import com.kafka.events.consumer.ConsumerContainer;
import com.kafka.events.consumer.ConsumerContainerManager;

import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;

/**
 * Reports on both halves of "is Kafka working here": the cluster is reachable, and this
 * application's consumers are actually consuming.
 *
 * <p>The second half is the one that catches real outages. A worker that died on an unrecoverable
 * error leaves its partitions unread while the process stays up and every other check passes, so
 * that condition is reported as DOWN rather than merely logged.
 */
public class KafkaHealthIndicator extends AbstractHealthIndicator {

	private final AdminService adminService;

	private final ConsumerContainerManager containerManager;

	public KafkaHealthIndicator(AdminService adminService, ConsumerContainerManager containerManager) {
		super("Kafka health check failed");
		this.adminService = adminService;
		this.containerManager = containerManager;
	}

	@Override
	protected void doHealthCheck(Health.Builder builder) throws Exception {
		AdminService.ClusterInfo cluster = this.adminService.describeCluster();
		builder.up()
			.withDetail("clusterId", cluster.clusterId())
			.withDetail("nodes", cluster.nodeCount())
			.withDetail("controller", cluster.controllerId());

		if (this.containerManager == null) {
			return;
		}
		Map<String, Object> consumers = new LinkedHashMap<>();
		long unhealthy = 0;
		for (ConsumerContainer container : this.containerManager.containers()) {
			consumers.put(container.id(),
					Map.of("running", container.isRunning(), "workers", container.concurrency(), "failedWorkers",
							container.failedWorkers()));
			if (!container.isHealthy()) {
				unhealthy++;
			}
		}
		if (consumers.isEmpty()) {
			return;
		}
		builder.withDetail("consumers", consumers);
		if (unhealthy > 0) {
			builder.down().withDetail("reason", unhealthy + " consumer container(s) are not consuming");
		}
	}

}
