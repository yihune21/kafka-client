package com.kafka.events.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "demo")
public class DemoProperties {

	/**
	 * Whether to publish and consume demo traffic on startup.
	 */
	private boolean enabled = true;

	private String topic = "events.demo";

	/**
	 * How many messages to publish at startup.
	 */
	private int messages = 10;

	/**
	 * Every Nth message is deliberately unprocessable, to exercise retries and dead-lettering.
	 */
	private int poisonEvery = 7;

	public boolean isEnabled() {
		return this.enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public String getTopic() {
		return this.topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getMessages() {
		return this.messages;
	}

	public void setMessages(int messages) {
		this.messages = messages;
	}

	public int getPoisonEvery() {
		return this.poisonEvery;
	}

	public void setPoisonEvery(int poisonEvery) {
		this.poisonEvery = poisonEvery;
	}

}
