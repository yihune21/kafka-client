package com.kafka.events;

import java.time.Duration;
import java.util.Map;

import com.kafka.events.admin.AdminService;
import com.kafka.events.config.KafkaAutoConfiguration;
import com.kafka.events.config.KafkaClientConfigFactory;
import com.kafka.events.config.KafkaProperties;
import com.kafka.events.config.TopicInitializer;
import com.kafka.events.consumer.ConsumerContainerManager;
import com.kafka.events.consumer.DeadLetterPublisher;
import com.kafka.events.consumer.TopicSubscription;
import com.kafka.events.producer.ProducerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Wiring tests. No broker is involved: the Kafka clients connect lazily, and with no topics
 * declared the topic initializer never issues a request.
 */
class KafkaAutoConfigurationTests {

	private final ApplicationContextRunner runner = new ApplicationContextRunner()
		.withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
		.withPropertyValues("kafka.bootstrap-servers=localhost:19092", "kafka.producer.close-timeout=1s",
				"kafka.admin.operation-timeout=2s");

	@Test
	void contributesTheCoreBeans() {
		this.runner.run((context) -> assertThat(context).hasSingleBean(KafkaClientConfigFactory.class)
			.hasSingleBean(AdminService.class)
			.hasSingleBean(TopicInitializer.class)
			.hasSingleBean(ProducerService.class)
			.hasSingleBean(DeadLetterPublisher.class)
			.hasSingleBean(ConsumerContainerManager.class));
	}

	@Test
	void masterSwitchRemovesEverything() {
		this.runner.withPropertyValues("kafka.enabled=false")
			.run((context) -> assertThat(context).doesNotHaveBean(AdminService.class)
				.doesNotHaveBean(ProducerService.class)
				.doesNotHaveBean(ConsumerContainerManager.class));
	}

	@Test
	void disablingTheProducerAlsoRemovesDeadLettering() {
		this.runner.withPropertyValues("kafka.producer.enabled=false")
			.run((context) -> assertThat(context).doesNotHaveBean(ProducerService.class)
				.doesNotHaveBean(DeadLetterPublisher.class)
				.hasSingleBean(AdminService.class));
	}

	@Test
	void disablingAdminRemovesTopicManagement() {
		this.runner.withPropertyValues("kafka.admin.enabled=false")
			.run((context) -> assertThat(context).doesNotHaveBean(AdminService.class)
				.doesNotHaveBean(TopicInitializer.class)
				.hasSingleBean(ProducerService.class));
	}

	@Test
	void applicationBeansOverrideTheDefaults() {
		AdminService replacement = mock(AdminService.class);
		this.runner.withBean("kafkaAdminService", AdminService.class, () -> replacement)
			.run((context) -> assertThat(context.getBean(AdminService.class)).isSameAs(replacement));
	}

	@Test
	void bindsDeclaredTopics() {
		// Admin is off here purely so the initializer does not go looking for a broker; the
		// binding under test happens either way.
		this.runner
			.withPropertyValues("kafka.admin.enabled=false", "kafka.admin.topics[0].name=orders",
					"kafka.admin.topics[0].partitions=6", "kafka.admin.topics[0].configs.retention.ms=604800000")
			.run((context) -> {
				KafkaProperties properties = context.getBean(KafkaProperties.class);
				assertThat(properties.getAdmin().getTopics()).singleElement().satisfies((topic) -> {
					assertThat(topic.name()).isEqualTo("orders");
					assertThat(topic.partitions()).isEqualTo(6);
					// -1 means "whatever the cluster considers correct", which is the right
					// default for anything that is not a single-node dev broker.
					assertThat(topic.replicationFactor()).isEqualTo((short) -1);
					assertThat(topic.configs()).containsEntry("retention.ms", "604800000");
				});
			});
	}

	@Test
	void aSubscriptionWithoutAGroupFailsWithAnActionableMessage() {
		this.runner
			.withBean("orphan", TopicSubscription.class,
					() -> TopicSubscription.builder().topics("orders").handler((record) -> {
					}).build())
			.run((context) -> assertThat(context).hasFailed()
				.getFailure()
				.rootCause()
				.hasMessageContaining("kafka.consumer.group-id"));
	}

	@Test
	void rejectsAnIdempotentProducerThatCouldReorderItsRetries() {
		this.runner.withPropertyValues("kafka.producer.max-in-flight-requests-per-connection=10")
			.run((context) -> assertThat(context).hasFailed()
				.getFailure()
				.rootCause()
				.hasMessageContaining("max-in-flight-requests-per-connection"));
	}

	@Test
	void rejectsADeliveryTimeoutShorterThanTheRetryWindow() {
		this.runner.withPropertyValues("kafka.producer.delivery-timeout=1s", "kafka.request-timeout=30s")
			.run((context) -> assertThat(context).hasFailed()
				.getFailure()
				.rootCause()
				.hasMessageContaining("delivery-timeout"));
	}

	@Test
	void consumersNeverAutoCommit() {
		this.runner.run((context) -> {
			Map<String, Object> config = context.getBean(KafkaClientConfigFactory.class)
				.consumerConfig("some-group", "some-client", Map.of());

			// The whole at-least-once story depends on this staying false: auto-commit would
			// acknowledge records before their handler ever ran.
			assertThat(config).containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
				.containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "some-group");
		});
	}

	@Test
	void producerDefaultsToDurableWrites() {
		this.runner.run((context) -> {
			Map<String, Object> config = context.getBean(KafkaClientConfigFactory.class).producerConfig();

			assertThat(config).containsEntry(ProducerConfig.ACKS_CONFIG, "all")
				.containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		});
	}

	@Test
	void perSubscriptionPropertiesWinOverGlobalOnes() {
		this.runner.withPropertyValues("kafka.consumer.max-poll-records=500").run((context) -> {
			Map<String, Object> config = context.getBean(KafkaClientConfigFactory.class)
				.consumerConfig("g", "c", Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"));

			assertThat(config).containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
		});
	}

}
