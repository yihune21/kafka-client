package com.kafka.events.app;

import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;

/**
 * The context must come up without a broker anywhere in sight, which is what
 * {@code kafka.enabled=false} is for. A build that needs infrastructure running to compile-check
 * its wiring is a build that fails on somebody's laptop.
 */
@SpringBootTest(properties = { "kafka.enabled=false", "demo.enabled=false" })
class EventsApplicationTests {

	@Test
	void contextLoads() {
	}

}
