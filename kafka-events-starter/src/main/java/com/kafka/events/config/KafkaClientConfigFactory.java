package com.kafka.events.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.util.StringUtils;

/**
 * Turns {@link KafkaProperties} into the config maps the Kafka clients expect.
 *
 * <p>Every value is written as a string and left for Kafka's own {@code ConfigDef} to parse, which
 * sidesteps the class of bug where a {@code long} is supplied for an {@code int} setting and the
 * client fails at construction with a type error rather than a useful message.
 *
 * <p>Configuration that Kafka would only reject deep inside client construction is checked up
 * front by {@link #validate()} instead, so a misconfiguration fails at startup with an explanation.
 */
public class KafkaClientConfigFactory {

	private final KafkaProperties properties;

	private final String baseClientId;

	public KafkaClientConfigFactory(KafkaProperties properties, String applicationName) {
		this.properties = properties;
		this.baseClientId = StringUtils.hasText(properties.getClientId()) ? properties.getClientId()
				: (StringUtils.hasText(applicationName) ? applicationName : "kafka-events");
		validate();
	}

	private void validate() {
		if (this.properties.getBootstrapServers() == null || this.properties.getBootstrapServers().isEmpty()) {
			throw new IllegalStateException("kafka.bootstrap-servers must not be empty");
		}

		KafkaProperties.Producer producer = this.properties.getProducer();
		if (producer.isIdempotence() && producer.getMaxInFlightRequestsPerConnection() > 5) {
			throw new IllegalStateException(
					"kafka.producer.max-in-flight-requests-per-connection must be 5 or less when idempotence is "
							+ "enabled (got " + producer.getMaxInFlightRequestsPerConnection()
							+ "); a higher value would let the broker reorder retried batches");
		}
		Duration minimumDeliveryTimeout = producer.getLinger().plus(this.properties.getRequestTimeout());
		if (producer.getDeliveryTimeout().compareTo(minimumDeliveryTimeout) < 0) {
			throw new IllegalStateException("kafka.producer.delivery-timeout (" + producer.getDeliveryTimeout()
					+ ") must be at least kafka.producer.linger + kafka.request-timeout (" + minimumDeliveryTimeout
					+ ")");
		}

		KafkaProperties.Consumer consumer = this.properties.getConsumer();
		if (consumer.getConcurrency() < 1) {
			throw new IllegalStateException(
					"kafka.consumer.concurrency must be at least 1, got " + consumer.getConcurrency());
		}
		if (consumer.getMaxPollRecords() < 1) {
			throw new IllegalStateException(
					"kafka.consumer.max-poll-records must be at least 1, got " + consumer.getMaxPollRecords());
		}
	}

	/**
	 * Client id for a single-instance client, for example {@code orders-service-producer}.
	 */
	public String clientId(String role) {
		return this.baseClientId + "-" + role;
	}

	/**
	 * Client id for one of several identical clients. Broker-side logs, quotas and lag dashboards
	 * all key off this, so each worker gets its own rather than sharing one.
	 */
	public String clientId(String role, int index) {
		return this.baseClientId + "-" + role + "-" + index;
	}

	public Map<String, Object> adminConfig() {
		Map<String, Object> config = base(clientId("admin"));
		config.putAll(this.properties.getAdmin().getProperties());
		return config;
	}

	public Map<String, Object> producerConfig() {
		KafkaProperties.Producer producer = this.properties.getProducer();
		Map<String, Object> config = base(clientId("producer"));
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		config.put(ProducerConfig.ACKS_CONFIG, producer.getAcks());
		config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(producer.isIdempotence()));
		config.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(producer.getRetries()));
		config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
				String.valueOf(producer.getMaxInFlightRequestsPerConnection()));
		config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producer.getCompressionType());
		config.put(ProducerConfig.LINGER_MS_CONFIG, millis(producer.getLinger()));
		config.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(producer.getBatchSize().toBytes()));
		config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(producer.getBufferMemory().toBytes()));
		config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, millis(producer.getDeliveryTimeout()));
		if (StringUtils.hasText(producer.getTransactionalId())) {
			config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producer.getTransactionalId());
		}
		config.putAll(producer.getProperties());
		return config;
	}

	/**
	 * Config for one consumer worker.
	 *
	 * @param groupId the group this worker joins
	 * @param clientId this worker's client id
	 * @param overrides per-subscription raw Kafka properties, applied last
	 */
	public Map<String, Object> consumerConfig(String groupId, String clientId, Map<String, String> overrides) {
		KafkaProperties.Consumer consumer = this.properties.getConsumer();
		Map<String, Object> config = base(clientId);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// Not configurable on purpose. This starter commits offsets after handlers succeed;
		// auto-commit would acknowledge records the application has not actually processed.
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumer.getAutoOffsetReset());
		config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(consumer.getMaxPollRecords()));
		config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, millis(consumer.getMaxPollInterval()));
		config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, consumer.getIsolationLevel());
		// Only set when explicitly configured: the KIP-848 group protocol rejects both settings,
		// so sending broker defaults along unprompted would break those consumers.
		if (consumer.getSessionTimeout() != null) {
			config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, millis(consumer.getSessionTimeout()));
		}
		if (consumer.getHeartbeatInterval() != null) {
			config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, millis(consumer.getHeartbeatInterval()));
		}
		if (StringUtils.hasText(consumer.getGroupProtocol())) {
			config.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, consumer.getGroupProtocol());
		}
		config.putAll(consumer.getProperties());
		config.putAll(overrides);
		return config;
	}

	private Map<String, Object> base(String clientId) {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", this.properties.getBootstrapServers()));
		config.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
		config.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, millis(this.properties.getRequestTimeout()));
		config.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, millis(this.properties.getMetadataMaxAge()));
		config.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, millis(this.properties.getReconnectBackoffMax()));
		applySecurity(config);
		config.putAll(this.properties.getProperties());
		return config;
	}

	private void applySecurity(Map<String, Object> config) {
		KafkaProperties.Security security = this.properties.getSecurity();
		config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, security.getProtocol());
		putIfPresent(config, SaslConfigs.SASL_MECHANISM, security.getSaslMechanism());
		putIfPresent(config, SaslConfigs.SASL_JAAS_CONFIG, security.getSaslJaasConfig());
		putIfPresent(config, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, security.getTruststoreLocation());
		putIfPresent(config, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, security.getTruststorePassword());
		putIfPresent(config, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, security.getKeystoreLocation());
		putIfPresent(config, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, security.getKeystorePassword());
		putIfPresent(config, SslConfigs.SSL_KEY_PASSWORD_CONFIG, security.getKeyPassword());
		// Distinct from the others: an empty string is a meaningful value here, it disables
		// hostname verification, so only a null means "leave at the default".
		if (security.getEndpointIdentificationAlgorithm() != null) {
			config.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
					security.getEndpointIdentificationAlgorithm());
		}
	}

	private static void putIfPresent(Map<String, Object> config, String key, String value) {
		if (StringUtils.hasText(value)) {
			config.put(key, value);
		}
	}

	private static String millis(Duration duration) {
		return String.valueOf(duration.toMillis());
	}

}
