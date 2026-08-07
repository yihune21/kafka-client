package com.kafka.events.config;

import java.util.List;

import com.kafka.events.admin.AdminService;
import com.kafka.events.consumer.ConsumerContainer;
import com.kafka.events.consumer.ConsumerContainerFactory;
import com.kafka.events.consumer.ConsumerContainerManager;
import com.kafka.events.consumer.DeadLetterPublisher;
import com.kafka.events.consumer.TopicSubscription;
import com.kafka.events.health.KafkaHealthIndicator;
import com.kafka.events.metrics.KafkaMetricsBinder;
import com.kafka.events.metrics.MicrometerKafkaMetricsBinder;
import com.kafka.events.producer.ProducerService;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

/**
 * Wires the whole starter from {@code kafka.*} properties.
 *
 * <p>Every bean is {@link ConditionalOnMissingBean}, so an application that needs something
 * different declares its own bean of that type and this configuration steps aside.
 *
 * <p>Shutdown ordering is deliberate. Consumer containers are a {@code SmartLifecycle} and stop
 * during {@code ContextClosedEvent}; the producer and admin client close later, as bean destruction
 * callbacks. That gap is what lets a worker finish dead-lettering a record on the way out.
 */
@AutoConfiguration
@ConditionalOnClass(KafkaProducer.class)
@ConditionalOnProperty(prefix = "kafka", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public KafkaClientConfigFactory kafkaClientConfigFactory(KafkaProperties properties, Environment environment) {
		return new KafkaClientConfigFactory(properties, environment.getProperty("spring.application.name"));
	}

	@Bean(destroyMethod = "close")
	@ConditionalOnMissingBean
	@ConditionalOnProperty(prefix = "kafka.admin", name = "enabled", matchIfMissing = true)
	public AdminService kafkaAdminService(KafkaProperties properties, KafkaClientConfigFactory configFactory) {
		return AdminService.create(configFactory.adminConfig(), properties.getAdmin().getOperationTimeout());
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(prefix = "kafka.admin", name = "enabled", matchIfMissing = true)
	public TopicInitializer kafkaTopicInitializer(AdminService adminService, KafkaProperties properties,
			ObjectProvider<TopicSubscription> subscriptions) {
		return new TopicInitializer(adminService, properties, subscriptions.orderedStream().toList());
	}

	@Bean(destroyMethod = "close")
	@ConditionalOnMissingBean
	@ConditionalOnProperty(prefix = "kafka.producer", name = "enabled", matchIfMissing = true)
	public ProducerService kafkaProducerService(KafkaProperties properties,
			KafkaClientConfigFactory configFactory, ObjectProvider<KafkaMetricsBinder> metricsBinder) {
		KafkaProperties.Producer producerProperties = properties.getProducer();
		Producer<String, String> producer = new KafkaProducer<>(configFactory.producerConfig());
		// The handle is intentionally not retained: these metrics live exactly as long as the
		// application context that owns both the producer and the registry.
		metricsBinder.getIfAvailable(KafkaMetricsBinder::noop)
			.bindProducer(producer, configFactory.clientId("producer"));
		return new ProducerService(producer, producerProperties.getCloseTimeout(),
				StringUtils.hasText(producerProperties.getTransactionalId()));
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(prefix = "kafka.producer", name = "enabled", matchIfMissing = true)
	public DeadLetterPublisher kafkaDeadLetterPublisher(ProducerService producerService,
			KafkaProperties properties) {
		return new DeadLetterPublisher(producerService, properties.getConsumer().getDeadLetter().getSuffix());
	}

	@Bean
	@ConditionalOnMissingBean
	public ConsumerContainerFactory kafkaConsumerContainerFactory(KafkaProperties properties,
			KafkaClientConfigFactory configFactory, ObjectProvider<DeadLetterPublisher> deadLetterPublisher,
			ObjectProvider<KafkaMetricsBinder> metricsBinder) {
		return new ConsumerContainerFactory(properties, configFactory, deadLetterPublisher.getIfAvailable(),
				metricsBinder.getIfAvailable(KafkaMetricsBinder::noop));
	}

	@Bean
	@ConditionalOnMissingBean
	public ConsumerContainerManager kafkaConsumerContainerManager(ConsumerContainerFactory containerFactory,
			ObjectProvider<TopicSubscription> subscriptions, KafkaProperties properties) {
		// Containers are built now but create no Kafka consumers until they are started, so a
		// disabled consumer section costs nothing more than a few objects.
		List<ConsumerContainer> containers = subscriptions.orderedStream().map(containerFactory::create).toList();
		return new ConsumerContainerManager(containers, properties.getConsumer().isEnabled());
	}

	/**
	 * Contributed only when Micrometer is present. Guarding the whole class rather than the method
	 * keeps {@link MeterRegistry} out of any signature Spring would have to resolve without it.
	 */
	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(MeterRegistry.class)
	static class MetricsConfiguration {

		@Bean
		@ConditionalOnMissingBean(KafkaMetricsBinder.class)
		KafkaMetricsBinder kafkaMetricsBinder(ObjectProvider<MeterRegistry> registry) {
			// ObjectProvider rather than @ConditionalOnBean: registry beans come from another
			// auto-configuration, and resolving at creation time avoids depending on which of the
			// two is evaluated first.
			MeterRegistry meterRegistry = registry.getIfAvailable();
			return (meterRegistry != null) ? new MicrometerKafkaMetricsBinder(meterRegistry)
					: KafkaMetricsBinder.noop();
		}

	}

	/**
	 * Contributed only when the application has actuator health on the classpath.
	 */
	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(HealthIndicator.class)
	static class HealthConfiguration {

		@Bean
		@ConditionalOnMissingBean(name = "kafkaHealthIndicator")
		@ConditionalOnProperty(prefix = "kafka.admin", name = "enabled", matchIfMissing = true)
		KafkaHealthIndicator kafkaHealthIndicator(AdminService adminService,
				ObjectProvider<ConsumerContainerManager> containerManager) {
			return new KafkaHealthIndicator(adminService, containerManager.getIfAvailable());
		}

	}

}
