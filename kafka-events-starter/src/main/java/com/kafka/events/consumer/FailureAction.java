package com.kafka.events.consumer;

/**
 * What a worker does with a record whose handler has exhausted its retries.
 *
 * <p>There is no universally right answer, which is why this is a choice rather than a default
 * buried in the runner: the correct behaviour depends on whether losing the record or halting the
 * partition is the worse outcome for the data in question.
 */
public enum FailureAction {

	/**
	 * Publish the record to a dead-letter topic together with the failure metadata, then commit
	 * past it and carry on. The offset is only committed once the dead-letter write is
	 * acknowledged, so the record is never dropped on the floor.
	 */
	DEAD_LETTER,

	/**
	 * Log the failure, commit past the record and carry on. The record is lost. Reasonable for
	 * genuinely disposable traffic such as metrics or cache warming.
	 */
	SKIP,

	/**
	 * Stop the worker without committing, so the record is redelivered when the container is
	 * restarted. Nothing is lost and nothing progresses; use it when silent data loss is worse
	 * than an outage that pages somebody.
	 */
	STOP

}
