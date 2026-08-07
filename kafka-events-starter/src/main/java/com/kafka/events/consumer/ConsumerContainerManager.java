package com.kafka.events.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.SmartLifecycle;

/**
 * Starts every consumer container once the context is fully refreshed, and stops them all when it
 * closes.
 *
 * <p>Being a {@link SmartLifecycle} rather than an {@code ApplicationRunner} is what makes shutdown
 * ordering correct: Spring runs lifecycle {@code stop()} before it destroys beans, so containers
 * have drained and committed while the producer they need for dead-lettering is still open.
 */
public class ConsumerContainerManager implements SmartLifecycle {

	/**
	 * Late enough to stop before most infrastructure beans, early enough to leave the web server's
	 * own graceful shutdown ahead of it.
	 */
	public static final int DEFAULT_PHASE = Integer.MAX_VALUE - 2048;

	private static final Logger log = LoggerFactory.getLogger(ConsumerContainerManager.class);

	private final List<ConsumerContainer> containers;

	private final boolean autoStart;

	private volatile boolean running;

	public ConsumerContainerManager(List<ConsumerContainer> containers, boolean autoStart) {
		this.containers = List.copyOf(containers);
		this.autoStart = autoStart;
	}

	@Override
	public void start() {
		if (this.running) {
			return;
		}
		if (this.containers.isEmpty()) {
			log.debug("No Kafka subscriptions declared; nothing to start");
		}
		this.containers.forEach(ConsumerContainer::start);
		this.running = true;
	}

	@Override
	public void stop() {
		if (!this.running) {
			return;
		}
		// Reverse order, mirroring startup, so anything that fans out to a later container is
		// already quiet by the time that container shuts down.
		List<ConsumerContainer> reversed = new ArrayList<>(this.containers);
		Collections.reverse(reversed);
		for (ConsumerContainer container : reversed) {
			try {
				container.stop();
			}
			catch (RuntimeException ex) {
				log.warn("Error stopping consumer container '{}'", container.id(), ex);
			}
		}
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStart;
	}

	@Override
	public int getPhase() {
		return DEFAULT_PHASE;
	}

	public List<ConsumerContainer> containers() {
		return this.containers;
	}

}
