package com.kafka.events.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Business logic for a single record.
 *
 * <p>The contract is deliberately narrow: return normally and the offset is committed, throw and
 * the record goes through the subscription's retry policy and then its {@link FailureAction}.
 * Handlers are called on the worker's polling thread, one record at a time, so they need no
 * synchronisation of their own but must not block indefinitely.
 *
 * <p>Redelivery is possible whenever a worker dies between handling a record and committing its
 * offset, so handlers should be idempotent.
 */
@FunctionalInterface
public interface MessageHandler {

	void handle(ConsumerRecord<String, String> record) throws Exception;

}
