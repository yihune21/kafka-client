package com.kafka.events.app;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.kafka.events.producer.ProducerService;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Publishes a burst of demo traffic at startup.
 *
 * <p>An {@link ApplicationRunner} runs after {@code SmartLifecycle} has started, so the consumer
 * containers are already polling by the time the first record is sent and nothing is racing.
 */
@Component
@ConditionalOnProperty(prefix = "demo", name = "enabled", matchIfMissing = true)
class DemoRunner implements ApplicationRunner {

	private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

	private final ProducerService producer;

	private final DemoProperties demo;

	DemoRunner(ProducerService producer, DemoProperties demo) {
		this.producer = producer;
		this.demo = demo;
	}

	@Override
	public void run(ApplicationArguments args) {
		String topic = this.demo.getTopic();
		List<CompletableFuture<RecordMetadata>> sent = new ArrayList<>();

		for (int index = 1; index <= this.demo.getMessages(); index++) {
			boolean poison = (this.demo.getPoisonEvery() > 0) && (index % this.demo.getPoisonEvery() == 0);
			String key = "order-" + index;
			String value = (poison ? "poison payload #" : "payload #") + index;
			sent.add(this.producer.send(topic, key, value));
		}

		// Waiting on every ack turns "we called send()" into "the brokers have it", which is the
		// difference this starter exists to make visible.
		CompletableFuture.allOf(sent.toArray(CompletableFuture[]::new)).join();
		log.info("published {} message(s) to '{}'; {} of them are deliberately unprocessable",
				this.demo.getMessages(), topic, poisonCount());
	}

	private int poisonCount() {
		return (this.demo.getPoisonEvery() > 0) ? this.demo.getMessages() / this.demo.getPoisonEvery() : 0;
	}

}
