package com.kafka.events;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import com.kafka.events.admin.AdminService;
import com.kafka.events.admin.TopicSpec;
import com.kafka.events.config.KafkaAutoConfiguration;
import com.kafka.events.consumer.DeadLetterPublisher;
import com.kafka.events.consumer.FailureAction;
import com.kafka.events.consumer.TopicSubscription;
import com.kafka.events.producer.ProducerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end against a real broker: publish, consume, exhaust retries, dead-letter, and read the
 * dead letter back.
 *
 * <p>Skipped rather than failed when Docker is unavailable, so the build still runs on a machine
 * that has none.
 */
@Testcontainers
@EnabledIf("dockerIsAvailable")
class KafkaEventsIntegrationTests {

	@Container
	private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.1");

	static boolean dockerIsAvailable() {
		try {
			return DockerClientFactory.instance().isDockerAvailable();
		}
		catch (Throwable ex) {
			return false;
		}
	}

	private ApplicationContextRunner runner() {
		return new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
			.withPropertyValues("kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
					"kafka.consumer.poll-timeout=250ms", "kafka.consumer.close-timeout=5s",
					"kafka.consumer.dead-letter.partitions=1", "kafka.admin.operation-timeout=20s");
	}

	@Test
	void deliversRetriesAndDeadLetters() {
		List<String> handled = new CopyOnWriteArrayList<>();
		List<String> deadLettered = new CopyOnWriteArrayList<>();
		AtomicInteger handlerCalls = new AtomicInteger();

		this.runner()
			.withPropertyValues("kafka.admin.topics[0].name=it.orders", "kafka.admin.topics[0].partitions=1",
					"kafka.consumer.retry.max-attempts=3", "kafka.consumer.retry.initial-backoff=50ms")
			.withBean("orders", TopicSubscription.class,
					() -> TopicSubscription.builder()
						.id("orders")
						.topics("it.orders")
						.groupId("it-orders")
						.handler((record) -> {
							if (record.value().startsWith("poison")) {
								handlerCalls.incrementAndGet();
								throw new IllegalStateException("cannot process " + record.value());
							}
							handled.add(record.value());
						})
						.build())
			.withBean("ordersDlt", TopicSubscription.class,
					() -> TopicSubscription.builder()
						.id("orders-dlt")
						.topics("it.orders.DLT")
						.groupId("it-orders-dlt")
						.failureAction(FailureAction.SKIP)
						.handler((record) -> deadLettered.add(record.value() + "|"
								+ header(record, DeadLetterPublisher.ORIGINAL_TOPIC) + "|"
								+ header(record, DeadLetterPublisher.ATTEMPTS)))
						.build())
			.run((context) -> {
				ProducerService producer = context.getBean(ProducerService.class);
				producer.sendAndWait("it.orders", "k1", "good-1");
				producer.sendAndWait("it.orders", "k2", "poison-1");
				producer.sendAndWait("it.orders", "k3", "good-2");

				waitFor(() -> handled.size() >= 2 && !deadLettered.isEmpty(), Duration.ofSeconds(60));

				assertThat(handled).containsExactlyInAnyOrder("good-1", "good-2");
				// Attempted the configured number of times before being given up on, not once.
				assertThat(handlerCalls).hasValue(3);
				assertThat(deadLettered).singleElement().isEqualTo("poison-1|it.orders|3");
			});
	}

	@Test
	void topicCreationIsIdempotent() {
		this.runner().run((context) -> {
			AdminService admin = context.getBean(AdminService.class);

			admin.ensureTopic(TopicSpec.of("it.idempotent", 2));
			// Running the same declaration again is what every second instance of a service does
			// on startup; it must not throw.
			admin.ensureTopic(TopicSpec.of("it.idempotent", 2));

			assertThat(admin.topicExists("it.idempotent")).isTrue();
			assertThat(admin.describeTopic("it.idempotent").partitions()).hasSize(2);
			assertThat(admin.describeCluster().nodeCount()).isEqualTo(1);
		});
	}

	@Test
	void reportsConsumerGroupLag() {
		this.runner().run((context) -> {
			AdminService admin = context.getBean(AdminService.class);
			admin.ensureTopic(TopicSpec.of("it.lag", 1));

			ProducerService producer = context.getBean(ProducerService.class);
			for (int index = 0; index < 5; index++) {
				producer.sendAndWait("it.lag", "k" + index, "v" + index);
			}

			// Nothing has consumed this topic, so the group has no committed offsets at all.
			assertThat(admin.consumerGroupLag("it-nobody")).isEmpty();
		});
	}

	private static String header(ConsumerRecord<String, String> record, String name) {
		Header header = record.headers().lastHeader(name);
		return (header != null) ? new String(header.value(), StandardCharsets.UTF_8) : "?";
	}

	private static void waitFor(BooleanSupplier condition, Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (System.nanoTime() < deadline) {
			if (condition.getAsBoolean()) {
				return;
			}
			Thread.sleep(100);
		}
		throw new AssertionError("Condition was not met within " + timeout);
	}

}
