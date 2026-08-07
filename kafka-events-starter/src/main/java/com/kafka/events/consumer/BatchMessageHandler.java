package com.kafka.events.consumer;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Business logic for a whole poll batch, for work that is much cheaper amortised across records:
 * a single bulk insert, one outbound request carrying many events.
 *
 * <p>Failure is all-or-nothing. If the call throws, the entire batch is retried, and on exhaustion
 * every record in it goes through the {@link FailureAction}. A batch that is partially applied and
 * then throws will therefore be re-applied, so batch handlers need to be idempotent for the same
 * reason single-record handlers do, only more so.
 */
@FunctionalInterface
public interface BatchMessageHandler {

	void handle(List<ConsumerRecord<String, String>> records) throws Exception;

}
