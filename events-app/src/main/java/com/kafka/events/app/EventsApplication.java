package com.kafka.events.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Demo application for {@code kafka-events-starter}.
 *
 * <p>Note what is not here: no {@code @ComponentScan} of the library, no manual client wiring. The
 * starter contributes its beans through auto-configuration, from a different package, driven
 * entirely by {@code application.yml}. That is the same path any other project takes.
 */
@SpringBootApplication
public class EventsApplication {

	public static void main(String[] args) {
		SpringApplication.run(EventsApplication.class, args);
	}

}
