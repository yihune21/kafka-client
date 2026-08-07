package com.kafka.events.app;

import java.nio.charset.StandardCharsets;

import com.kafka.events.consumer.DeadLetterPublisher;
import com.kafka.events.consumer.FailureAction;
import com.kafka.events.consumer.RetryPolicy;
import com.kafka.events.consumer.TopicSubscription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static java.time.Duration.ofMillis;

/**
 * Declares what this application consumes. Publishing a {@link TopicSubscription} bean is the whole
 * integration: the starter builds a container for it, starts it after the topics exist, and stops
 * it before the producer closes.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DemoProperties.class)
@ConditionalOnProperty(prefix = "demo", name = "enabled", matchIfMissing = true)
class DemoConfiguration {

	private static final Logger log = LoggerFactory.getLogger(DemoConfiguration.class);

	/**
	 * The ordinary path: three workers over the topic's three partitions, retrying a failed record
	 * twice before it is dead-lettered.
	 */
	@Bean
	TopicSubscription demoSubscription(DemoProperties demo) {
		return TopicSubscription.builder()
			.id("demo")
			.topics(demo.getTopic())
			.concurrency(3)
			.retryPolicy(new RetryPolicy(3, ofMillis(200), 2.0, ofMillis(2000)))
			.handler(DemoConfiguration::handle)
			.build();
	}

	/**
	 * Consumes the dead-letter topic, which is how you would actually operate this: something has
	 * to look at what failed.
	 *
	 * <p>Failures here SKIP rather than dead-letter, because a dead-letter topic for the
	 * dead-letter topic just moves the problem one hop further away.
	 */
	@Bean
	TopicSubscription demoDeadLetterSubscription(DemoProperties demo) {
		return TopicSubscription.builder()
			.id("demo-dlt")
			.topics(demo.getTopic() + ".DLT")
			.groupId("events-demo-dlt")
			.failureAction(FailureAction.SKIP)
			.handler(DemoConfiguration::inspectDeadLetter)
			.build();
	}

	private static void handle(ConsumerRecord<String, String> record) {
		if (record.value().startsWith("poison")) {
			throw new IllegalStateException("Cannot process " + record.value());
		}
		log.info("handled {} from {}-{}@{}: {}", record.key(), record.topic(), record.partition(), record.offset(),
				record.value());
	}

	private static void inspectDeadLetter(ConsumerRecord<String, String> record) {
		log.warn("dead letter {}: originally {}-{}@{}, failed {} time(s) with {}", record.key(),
				header(record, DeadLetterPublisher.ORIGINAL_TOPIC),
				header(record, DeadLetterPublisher.ORIGINAL_PARTITION),
				header(record, DeadLetterPublisher.ORIGINAL_OFFSET), header(record, DeadLetterPublisher.ATTEMPTS),
				header(record, DeadLetterPublisher.EXCEPTION_MESSAGE));
	}

	private static String header(ConsumerRecord<String, String> record, String name) {
		Header header = record.headers().lastHeader(name);
		return (header != null) ? new String(header.value(), StandardCharsets.UTF_8) : "?";
	}

}
