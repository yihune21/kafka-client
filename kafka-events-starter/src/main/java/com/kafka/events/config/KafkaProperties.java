package com.kafka.events.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.kafka.events.admin.TopicSpec;
import com.kafka.events.consumer.FailureAction;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

/**
 * Everything this starter needs, under the {@code kafka.*} prefix.
 *
 * <p>The defaults are the ones you would pick for a production service: acknowledged writes,
 * idempotent producers, and offsets committed only after a handler has actually succeeded. A
 * development cluster works with nothing set but {@code kafka.bootstrap-servers}.
 *
 * <p>Anything not modelled here can still be set through the {@code properties} maps, which are
 * passed to the Kafka client verbatim. Precedence, lowest to highest: computed defaults, then
 * {@code kafka.properties}, then the per-client {@code kafka.<client>.properties}, then
 * per-subscription overrides.
 */
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

	/**
	 * Master switch. Set to false to leave every bean in this starter unregistered, which is the
	 * cleanest way to run a test or a local profile with no broker at all.
	 */
	private boolean enabled = true;

	/**
	 * Broker addresses used by every client this starter creates.
	 */
	private List<String> bootstrapServers = new ArrayList<>(List.of("localhost:9092"));

	/**
	 * Base client id. Each client appends its own role and index, giving broker-side logs and
	 * quotas something meaningful to attribute traffic to. Defaults to spring.application.name.
	 */
	private String clientId;

	/**
	 * How long a client waits for a response before retrying the request.
	 */
	private Duration requestTimeout = Duration.ofSeconds(30);

	/**
	 * How long cluster metadata may be cached before a refresh is forced.
	 */
	private Duration metadataMaxAge = Duration.ofMinutes(5);

	/**
	 * Upper bound on the exponential backoff between reconnect attempts to a broker.
	 */
	private Duration reconnectBackoffMax = Duration.ofSeconds(10);

	private final Security security = new Security();

	private final Admin admin = new Admin();

	private final Producer producer = new Producer();

	private final Consumer consumer = new Consumer();

	/**
	 * Raw Kafka properties applied to every client. Escape hatch for anything not modelled above.
	 */
	private final Map<String, String> properties = new LinkedHashMap<>();

	public boolean isEnabled() {
		return this.enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public List<String> getBootstrapServers() {
		return this.bootstrapServers;
	}

	public void setBootstrapServers(List<String> bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getClientId() {
		return this.clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public Duration getRequestTimeout() {
		return this.requestTimeout;
	}

	public void setRequestTimeout(Duration requestTimeout) {
		this.requestTimeout = requestTimeout;
	}

	public Duration getMetadataMaxAge() {
		return this.metadataMaxAge;
	}

	public void setMetadataMaxAge(Duration metadataMaxAge) {
		this.metadataMaxAge = metadataMaxAge;
	}

	public Duration getReconnectBackoffMax() {
		return this.reconnectBackoffMax;
	}

	public void setReconnectBackoffMax(Duration reconnectBackoffMax) {
		this.reconnectBackoffMax = reconnectBackoffMax;
	}

	public Security getSecurity() {
		return this.security;
	}

	public Admin getAdmin() {
		return this.admin;
	}

	public Producer getProducer() {
		return this.producer;
	}

	public Consumer getConsumer() {
		return this.consumer;
	}

	public Map<String, String> getProperties() {
		return this.properties;
	}

	/**
	 * Transport security. The defaults describe a plaintext development broker; a managed cluster
	 * typically needs {@code protocol: SASL_SSL} plus a mechanism and JAAS config.
	 */
	public static class Security {

		/**
		 * Kafka security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL.
		 */
		private String protocol = "PLAINTEXT";

		/**
		 * SASL mechanism, for example PLAIN, SCRAM-SHA-512 or AWS_MSK_IAM.
		 */
		private String saslMechanism;

		/**
		 * SASL JAAS configuration line. Keep the secret in an env var, not in a committed file.
		 */
		private String saslJaasConfig;

		private String truststoreLocation;

		private String truststorePassword;

		private String keystoreLocation;

		private String keystorePassword;

		private String keyPassword;

		/**
		 * Set to an empty string to disable server hostname verification. Only ever appropriate
		 * against a development broker.
		 */
		private String endpointIdentificationAlgorithm;

		public String getProtocol() {
			return this.protocol;
		}

		public void setProtocol(String protocol) {
			this.protocol = protocol;
		}

		public String getSaslMechanism() {
			return this.saslMechanism;
		}

		public void setSaslMechanism(String saslMechanism) {
			this.saslMechanism = saslMechanism;
		}

		public String getSaslJaasConfig() {
			return this.saslJaasConfig;
		}

		public void setSaslJaasConfig(String saslJaasConfig) {
			this.saslJaasConfig = saslJaasConfig;
		}

		public String getTruststoreLocation() {
			return this.truststoreLocation;
		}

		public void setTruststoreLocation(String truststoreLocation) {
			this.truststoreLocation = truststoreLocation;
		}

		public String getTruststorePassword() {
			return this.truststorePassword;
		}

		public void setTruststorePassword(String truststorePassword) {
			this.truststorePassword = truststorePassword;
		}

		public String getKeystoreLocation() {
			return this.keystoreLocation;
		}

		public void setKeystoreLocation(String keystoreLocation) {
			this.keystoreLocation = keystoreLocation;
		}

		public String getKeystorePassword() {
			return this.keystorePassword;
		}

		public void setKeystorePassword(String keystorePassword) {
			this.keystorePassword = keystorePassword;
		}

		public String getKeyPassword() {
			return this.keyPassword;
		}

		public void setKeyPassword(String keyPassword) {
			this.keyPassword = keyPassword;
		}

		public String getEndpointIdentificationAlgorithm() {
			return this.endpointIdentificationAlgorithm;
		}

		public void setEndpointIdentificationAlgorithm(String endpointIdentificationAlgorithm) {
			this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
		}

	}

	public static class Admin {

		/**
		 * Whether to create an AdminClient at all. Disable in services that only produce to
		 * topics somebody else owns.
		 */
		private boolean enabled = true;

		/**
		 * How long to wait for a single admin operation before giving up.
		 */
		private Duration operationTimeout = Duration.ofSeconds(30);

		/**
		 * Topics ensured at startup, before any consumer starts polling. Creation is idempotent:
		 * existing topics are left exactly as they are.
		 */
		private List<TopicSpec> topics = new ArrayList<>();

		/**
		 * Whether a topic that cannot be created should fail application startup. Leaving this on
		 * is what stops a service from coming up healthy and then silently dropping traffic.
		 */
		private boolean failFast = true;

		private final Map<String, String> properties = new LinkedHashMap<>();

		public boolean isEnabled() {
			return this.enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public Duration getOperationTimeout() {
			return this.operationTimeout;
		}

		public void setOperationTimeout(Duration operationTimeout) {
			this.operationTimeout = operationTimeout;
		}

		public List<TopicSpec> getTopics() {
			return this.topics;
		}

		public void setTopics(List<TopicSpec> topics) {
			this.topics = topics;
		}

		public boolean isFailFast() {
			return this.failFast;
		}

		public void setFailFast(boolean failFast) {
			this.failFast = failFast;
		}

		public Map<String, String> getProperties() {
			return this.properties;
		}

	}

	public static class Producer {

		/**
		 * Whether to create a producer. Dead-letter publishing needs one, so a consumer using the
		 * DEAD_LETTER failure action requires this to stay enabled.
		 */
		private boolean enabled = true;

		/**
		 * Number of in-sync replicas that must acknowledge a write. "all" is the only setting
		 * that does not trade durability for latency.
		 */
		private String acks = "all";

		/**
		 * Whether the broker deduplicates retried writes. Turning this off reintroduces duplicate
		 * records on retry.
		 */
		private boolean idempotence = true;

		private int retries = Integer.MAX_VALUE;

		/**
		 * Kept at or below 5, which is what the idempotent producer supports while still
		 * guaranteeing ordering per partition.
		 */
		private int maxInFlightRequestsPerConnection = 5;

		/**
		 * Batch compression: none, gzip, snappy, lz4 or zstd.
		 */
		private String compressionType = "lz4";

		/**
		 * How long to wait for a batch to fill before sending it. A few milliseconds buys a large
		 * throughput win; zero optimises purely for latency.
		 */
		private Duration linger = Duration.ofMillis(20);

		private DataSize batchSize = DataSize.ofKilobytes(32);

		/**
		 * Memory the producer may use to buffer records awaiting send. Once exhausted, send()
		 * blocks for max.block.ms and then fails.
		 */
		private DataSize bufferMemory = DataSize.ofMegabytes(32);

		/**
		 * Total time a record may spend between send() and a success or permanent failure,
		 * covering all retries. Must be at least linger + requestTimeout.
		 */
		private Duration deliveryTimeout = Duration.ofMinutes(2);

		/**
		 * How long close() waits for in-flight records to be acknowledged before dropping them.
		 */
		private Duration closeTimeout = Duration.ofSeconds(30);

		/**
		 * Set to enable transactions. Must be stable across restarts and unique per producer
		 * instance, otherwise instances fence each other.
		 */
		private String transactionalId;

		private final Map<String, String> properties = new LinkedHashMap<>();

		public boolean isEnabled() {
			return this.enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public String getAcks() {
			return this.acks;
		}

		public void setAcks(String acks) {
			this.acks = acks;
		}

		public boolean isIdempotence() {
			return this.idempotence;
		}

		public void setIdempotence(boolean idempotence) {
			this.idempotence = idempotence;
		}

		public int getRetries() {
			return this.retries;
		}

		public void setRetries(int retries) {
			this.retries = retries;
		}

		public int getMaxInFlightRequestsPerConnection() {
			return this.maxInFlightRequestsPerConnection;
		}

		public void setMaxInFlightRequestsPerConnection(int maxInFlightRequestsPerConnection) {
			this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
		}

		public String getCompressionType() {
			return this.compressionType;
		}

		public void setCompressionType(String compressionType) {
			this.compressionType = compressionType;
		}

		public Duration getLinger() {
			return this.linger;
		}

		public void setLinger(Duration linger) {
			this.linger = linger;
		}

		public DataSize getBatchSize() {
			return this.batchSize;
		}

		public void setBatchSize(DataSize batchSize) {
			this.batchSize = batchSize;
		}

		public DataSize getBufferMemory() {
			return this.bufferMemory;
		}

		public void setBufferMemory(DataSize bufferMemory) {
			this.bufferMemory = bufferMemory;
		}

		public Duration getDeliveryTimeout() {
			return this.deliveryTimeout;
		}

		public void setDeliveryTimeout(Duration deliveryTimeout) {
			this.deliveryTimeout = deliveryTimeout;
		}

		public Duration getCloseTimeout() {
			return this.closeTimeout;
		}

		public void setCloseTimeout(Duration closeTimeout) {
			this.closeTimeout = closeTimeout;
		}

		public String getTransactionalId() {
			return this.transactionalId;
		}

		public void setTransactionalId(String transactionalId) {
			this.transactionalId = transactionalId;
		}

		public Map<String, String> getProperties() {
			return this.properties;
		}

	}

	public static class Consumer {

		/**
		 * Whether declared subscriptions are started. Disabling leaves the beans in place but
		 * polls nothing, which is handy for a write-only instance of a service.
		 */
		private boolean enabled = true;

		/**
		 * Default consumer group for subscriptions that do not name their own.
		 */
		private String groupId;

		/**
		 * Where to start when a group has no committed offset: earliest, latest or none.
		 */
		private String autoOffsetReset = "earliest";

		/**
		 * How long each poll blocks waiting for records. Also the granularity at which a worker
		 * notices a shutdown request.
		 */
		private Duration pollTimeout = Duration.ofSeconds(1);

		/**
		 * Maximum records returned by a single poll. Lower this if handlers are slow, so a batch
		 * still finishes well inside maxPollInterval.
		 */
		private int maxPollRecords = 500;

		/**
		 * Maximum time between polls before the broker assumes the consumer is dead and rebalances
		 * its partitions away. Worker threads keep polling a paused consumer during retry backoff
		 * so that slow retries do not trip this.
		 */
		private Duration maxPollInterval = Duration.ofMinutes(5);

		/**
		 * Session timeout, applicable to the classic group protocol only. Left unset by default so
		 * that the broker default applies, and so consumers on the KIP-848 protocol are not
		 * rejected for setting it.
		 */
		private Duration sessionTimeout;

		/**
		 * Heartbeat interval, classic group protocol only. See sessionTimeout.
		 */
		private Duration heartbeatInterval;

		/**
		 * Group protocol: classic or consumer (KIP-848). Left unset to take the client default.
		 */
		private String groupProtocol;

		/**
		 * Worker threads per subscription. Each gets its own consumer, so this is capped in
		 * practice by the partition count of the topics it subscribes to.
		 */
		private int concurrency = 1;

		/**
		 * How long a worker waits to leave its group cleanly during shutdown.
		 */
		private Duration closeTimeout = Duration.ofSeconds(30);

		/**
		 * read_committed hides records from aborted transactions; read_uncommitted shows
		 * everything.
		 */
		private String isolationLevel = "read_committed";

		/**
		 * What to do with a record whose handler has exhausted its retries.
		 */
		private FailureAction onFailure = FailureAction.DEAD_LETTER;

		private final Retry retry = new Retry();

		private final DeadLetter deadLetter = new DeadLetter();

		private final Map<String, String> properties = new LinkedHashMap<>();

		public boolean isEnabled() {
			return this.enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public String getGroupId() {
			return this.groupId;
		}

		public void setGroupId(String groupId) {
			this.groupId = groupId;
		}

		public String getAutoOffsetReset() {
			return this.autoOffsetReset;
		}

		public void setAutoOffsetReset(String autoOffsetReset) {
			this.autoOffsetReset = autoOffsetReset;
		}

		public Duration getPollTimeout() {
			return this.pollTimeout;
		}

		public void setPollTimeout(Duration pollTimeout) {
			this.pollTimeout = pollTimeout;
		}

		public int getMaxPollRecords() {
			return this.maxPollRecords;
		}

		public void setMaxPollRecords(int maxPollRecords) {
			this.maxPollRecords = maxPollRecords;
		}

		public Duration getMaxPollInterval() {
			return this.maxPollInterval;
		}

		public void setMaxPollInterval(Duration maxPollInterval) {
			this.maxPollInterval = maxPollInterval;
		}

		public Duration getSessionTimeout() {
			return this.sessionTimeout;
		}

		public void setSessionTimeout(Duration sessionTimeout) {
			this.sessionTimeout = sessionTimeout;
		}

		public Duration getHeartbeatInterval() {
			return this.heartbeatInterval;
		}

		public void setHeartbeatInterval(Duration heartbeatInterval) {
			this.heartbeatInterval = heartbeatInterval;
		}

		public String getGroupProtocol() {
			return this.groupProtocol;
		}

		public void setGroupProtocol(String groupProtocol) {
			this.groupProtocol = groupProtocol;
		}

		public int getConcurrency() {
			return this.concurrency;
		}

		public void setConcurrency(int concurrency) {
			this.concurrency = concurrency;
		}

		public Duration getCloseTimeout() {
			return this.closeTimeout;
		}

		public void setCloseTimeout(Duration closeTimeout) {
			this.closeTimeout = closeTimeout;
		}

		public String getIsolationLevel() {
			return this.isolationLevel;
		}

		public void setIsolationLevel(String isolationLevel) {
			this.isolationLevel = isolationLevel;
		}

		public FailureAction getOnFailure() {
			return this.onFailure;
		}

		public void setOnFailure(FailureAction onFailure) {
			this.onFailure = onFailure;
		}

		public Retry getRetry() {
			return this.retry;
		}

		public DeadLetter getDeadLetter() {
			return this.deadLetter;
		}

		public Map<String, String> getProperties() {
			return this.properties;
		}

	}

	public static class Retry {

		/**
		 * Total attempts per record, including the first. 1 disables retrying.
		 */
		private int maxAttempts = 3;

		private Duration initialBackoff = Duration.ofSeconds(1);

		/**
		 * Factor applied to the backoff after each failed attempt.
		 */
		private double multiplier = 2.0;

		private Duration maxBackoff = Duration.ofSeconds(30);

		public int getMaxAttempts() {
			return this.maxAttempts;
		}

		public void setMaxAttempts(int maxAttempts) {
			this.maxAttempts = maxAttempts;
		}

		public Duration getInitialBackoff() {
			return this.initialBackoff;
		}

		public void setInitialBackoff(Duration initialBackoff) {
			this.initialBackoff = initialBackoff;
		}

		public double getMultiplier() {
			return this.multiplier;
		}

		public void setMultiplier(double multiplier) {
			this.multiplier = multiplier;
		}

		public Duration getMaxBackoff() {
			return this.maxBackoff;
		}

		public void setMaxBackoff(Duration maxBackoff) {
			this.maxBackoff = maxBackoff;
		}

	}

	public static class DeadLetter {

		/**
		 * Appended to the source topic name to derive the dead-letter topic.
		 */
		private String suffix = ".DLT";

		/**
		 * Partition count used when auto-creating a dead-letter topic.
		 */
		private int partitions = 3;

		/**
		 * Whether dead-letter topics for declared topics are created at startup. Creating them up
		 * front matters because a broker with auto-creation disabled will otherwise reject the
		 * very write that was meant to rescue a poisoned record.
		 */
		private boolean autoCreate = true;

		public String getSuffix() {
			return this.suffix;
		}

		public void setSuffix(String suffix) {
			this.suffix = suffix;
		}

		public int getPartitions() {
			return this.partitions;
		}

		public void setPartitions(int partitions) {
			this.partitions = partitions;
		}

		public boolean isAutoCreate() {
			return this.autoCreate;
		}

		public void setAutoCreate(boolean autoCreate) {
			this.autoCreate = autoCreate;
		}

	}

}
