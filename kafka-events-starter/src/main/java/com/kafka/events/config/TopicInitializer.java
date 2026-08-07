package com.kafka.events.config;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.kafka.events.admin.AdminService;
import com.kafka.events.admin.TopicSpec;
import com.kafka.events.consumer.FailureAction;
import com.kafka.events.consumer.TopicSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.InitializingBean;

/**
 * Creates the topics this application declares, before anything tries to use them.
 *
 * <p>Runs during context refresh, which puts it ahead of
 * {@link com.kafka.events.consumer.ConsumerContainerManager} starting any consumer, since
 * {@code SmartLifecycle} only starts once every singleton is initialised.
 *
 * <p>Dead-letter topics are created alongside their sources. A cluster with auto-creation disabled
 * would otherwise reject the dead-letter write at exactly the moment it matters, turning a handler
 * failure into a stuck partition.
 */
public class TopicInitializer implements InitializingBean {

	private static final Logger log = LoggerFactory.getLogger(TopicInitializer.class);

	private final AdminService adminService;

	private final KafkaProperties properties;

	private final List<TopicSubscription> subscriptions;

	public TopicInitializer(AdminService adminService, KafkaProperties properties,
			List<TopicSubscription> subscriptions) {
		this.adminService = adminService;
		this.properties = properties;
		this.subscriptions = subscriptions;
	}

	@Override
	public void afterPropertiesSet() {
		ensureTopics();
	}

	public void ensureTopics() {
		// Keyed by name with putIfAbsent, so an explicit declaration always beats a derived one.
		Map<String, TopicSpec> specs = new LinkedHashMap<>();
		this.properties.getAdmin().getTopics().forEach((spec) -> specs.putIfAbsent(spec.name(), spec));

		KafkaProperties.DeadLetter deadLetter = this.properties.getConsumer().getDeadLetter();
		if (deadLetter.isAutoCreate()) {
			deadLetterTopicNames(deadLetter)
				.forEach((name) -> specs.putIfAbsent(name, TopicSpec.of(name, deadLetter.getPartitions())));
		}

		if (specs.isEmpty()) {
			return;
		}
		log.debug("Ensuring Kafka topics {}", specs.keySet());
		try {
			this.adminService.ensureTopics(specs.values());
		}
		catch (RuntimeException ex) {
			if (this.properties.getAdmin().isFailFast()) {
				throw ex;
			}
			log.warn("Could not ensure topics {}; continuing because kafka.admin.fail-fast is false", specs.keySet(),
					ex);
		}
	}

	private Set<String> deadLetterTopicNames(KafkaProperties.DeadLetter deadLetter) {
		String suffix = deadLetter.getSuffix();
		Set<String> names = new LinkedHashSet<>();

		for (TopicSpec spec : this.properties.getAdmin().getTopics()) {
			if (!spec.name().endsWith(suffix)) {
				names.add(spec.name() + suffix);
			}
		}
		for (TopicSubscription subscription : this.subscriptions) {
			if (!deadLetters(subscription)) {
				continue;
			}
			if (subscription.deadLetterTopic() != null) {
				names.add(subscription.deadLetterTopic());
				continue;
			}
			// A pattern subscription has no fixed topic names to derive from, so its dead-letter
			// topics cannot be created up front; declare them under kafka.admin.topics instead.
			subscription.topics().stream().filter((topic) -> !topic.endsWith(suffix)).forEach(
					(topic) -> names.add(topic + suffix));
		}
		return names;
	}

	private boolean deadLetters(TopicSubscription subscription) {
		FailureAction action = (subscription.failureAction() != null) ? subscription.failureAction()
				: this.properties.getConsumer().getOnFailure();
		return action == FailureAction.DEAD_LETTER;
	}

}
