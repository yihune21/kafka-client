# kafka-events-starter

A Spring Boot starter over the raw Kafka client API, meant to be dropped into any service that
needs Kafka. It handles the parts that are easy to get subtly wrong — offset commits, retries,
dead-lettering, graceful shutdown — and leaves the client API itself visible for anything else.

```
events/
├── kafka-events-starter/   the reusable library (publish this)
├── events-app/             a demo app that consumes it like any other project would
└── docker-compose.yml      single-node broker for local development
```

## Using it in another project

Install it once (`./mvnw install`), then:

```xml
<dependency>
    <groupId>com.kafka</groupId>
    <artifactId>kafka-events-starter</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

```yaml
kafka:
  bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
  admin:
    topics:
      - name: orders
        partitions: 6
  consumer:
    group-id: orders-service
```

Publishing:

```java
@Service
class OrderService {

    private final ProducerService producer;   // injected, no configuration needed

    void place(Order order) {
        producer.send("orders", order.id(), toJson(order))
            .whenComplete((metadata, failure) -> { /* ... */ });
    }
}
```

Consuming — declare a `TopicSubscription` bean and the starter builds, starts and stops a container
for it:

```java
@Bean
TopicSubscription orders(OrderHandler handler) {
    return TopicSubscription.builder()
        .topics("orders")
        .concurrency(3)
        .handler(handler::handle)   // throw to trigger retry, then dead-lettering
        .build();
}
```

That is the whole integration. No component scanning of the library, no manual client construction;
`events-app` lives in a different package and wires up purely through auto-configuration.

## What it does for you

**Offsets follow your handler, not the clock.** `enable.auto.commit` is forced to `false` and is not
configurable. A record's offset is committed only after its handler returns normally, so a worker
that dies mid-batch causes redelivery rather than silent loss. Delivery is at-least-once, which
means **handlers must be idempotent**.

**Retries do not get you evicted from the group.** Sleeping between retries would stall `poll()`,
and once the gap exceeds `max.poll.interval.ms` the broker rebalances your partitions away —
usually while every other worker is backing off too. During backoff the worker pauses its
assignment and keeps polling, which holds group membership without consuming anything.

**Dead letters are written before the offset moves.** `DeadLetterPublisher` blocks on the ack. If
the dead-letter write fails, the source offset stays uncommitted and the record comes back, rather
than disappearing. Dead-letter topics are created at startup alongside their sources, because a
cluster with auto-creation off would otherwise reject that write at exactly the wrong moment.

**Failure is a policy, not a default.** `kafka.consumer.on-failure` is `dead-letter`, `skip`
(drop it, for genuinely disposable traffic) or `stop` (halt without committing, when silent loss is
worse than an outage).

**Shutdown is ordered.** Consumer containers are a `SmartLifecycle` and stop on `ContextClosedEvent`;
the producer and admin client close afterwards during bean destruction. That gap is what lets a
worker finish dead-lettering on the way out. Verified in the demo: all six workers stop, then
`Closing Kafka producer`.

**Topic creation is checked.** `AdminClient.createTopics` hands back a future that is easy to drop;
every admin call here is awaited and converted to an exception. Creation is idempotent, so every
instance of a service can run it at startup. Existing topics are never reshaped — a partition-count
mismatch is warned about, not "fixed", because changing it remaps keys and breaks per-key ordering.

**Health and metrics if you want them.** With actuator on the classpath, `/actuator/health` reports
cluster identity plus per-container worker state, and goes DOWN when a worker has died while the
process stays up. With Micrometer, the Kafka client metrics (including consumer lag) are registered
automatically. Both are optional dependencies; without them the starter loads unchanged.

## Configuration

Everything lives under `kafka.*`; `KafkaProperties` documents each key and your IDE will complete
them. The defaults are production defaults: `acks=all`, idempotent producer, `read_committed`,
lz4 compression, retries with exponential backoff.

Notable keys:

| Key | Default | Notes |
| --- | --- | --- |
| `kafka.enabled` | `true` | Master switch; `false` registers nothing (useful in tests) |
| `kafka.bootstrap-servers` | `localhost:9092` | |
| `kafka.admin.topics[*]` | — | Topics ensured at startup |
| `kafka.admin.fail-fast` | `true` | Fail startup if a declared topic cannot be created |
| `kafka.producer.acks` | `all` | |
| `kafka.producer.transactional-id` | — | Set to enable `runInTransaction` |
| `kafka.consumer.group-id` | — | Required unless each subscription names its own |
| `kafka.consumer.concurrency` | `1` | Workers per subscription; capped by partitions |
| `kafka.consumer.on-failure` | `dead-letter` | or `skip`, `stop` |
| `kafka.consumer.retry.*` | 3 attempts, 1s, ×2 | |
| `kafka.security.*` | plaintext | `protocol: SASL_SSL` + mechanism for a managed cluster |

Anything not modelled goes through the escape-hatch maps, applied in order: `kafka.properties` →
`kafka.<client>.properties` → per-subscription `property(...)`.

## Local broker

```bash
docker compose up -d broker            # add --profile tools for a UI on :8085
```

The compose file publishes `9092` and advertises two listeners: `localhost:9092` for apps on the
host, `broker:29092` for other containers on the network. The previous configuration bound
`localhost` *inside* the container and published no ports, so nothing outside could ever connect.
Auto topic creation is off, so a typo produces an error rather than a stray topic.

## Running the demo

```bash
./mvnw install
java -jar events-app/target/events-app-0.0.1-SNAPSHOT.jar
```

It publishes 10 messages, one of which the handler refuses. Expect to see nine handled, that one
retried three times and routed to `events.demo.DLT`, and a second subscription reading it back with
the original topic, partition, offset and exception attached.

## Tests

`./mvnw test` runs wiring tests with no broker (the clients connect lazily) plus a Testcontainers
suite that exercises publish → consume → retry → dead-letter against a real broker. The
Testcontainers tests skip themselves when Docker is unavailable rather than failing the build.
