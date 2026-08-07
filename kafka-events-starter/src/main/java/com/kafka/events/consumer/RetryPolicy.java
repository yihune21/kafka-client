package com.kafka.events.consumer;

import java.time.Duration;

/**
 * Exponential backoff schedule applied to a failing handler, in place, before the record is handed
 * to the configured {@link FailureAction}.
 *
 * @param maxAttempts total attempts including the first; 1 disables retrying
 * @param initialBackoff delay after the first failure
 * @param multiplier factor applied to the delay after each subsequent failure
 * @param maxBackoff ceiling on any single delay
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff, double multiplier, Duration maxBackoff) {

	public RetryPolicy {
		if (maxAttempts < 1) {
			throw new IllegalArgumentException("maxAttempts must be at least 1, got " + maxAttempts);
		}
		if (initialBackoff == null || initialBackoff.isNegative()) {
			throw new IllegalArgumentException("initialBackoff must be zero or positive");
		}
		if (multiplier < 1.0) {
			throw new IllegalArgumentException("multiplier must be at least 1.0, got " + multiplier);
		}
		if (maxBackoff == null || maxBackoff.isNegative()) {
			throw new IllegalArgumentException("maxBackoff must be zero or positive");
		}
	}

	/**
	 * Retry twice more after the first failure, one second apart, doubling.
	 */
	public static RetryPolicy defaults() {
		return new RetryPolicy(3, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30));
	}

	/**
	 * Hand the first failure straight to the failure action.
	 */
	public static RetryPolicy none() {
		return new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
	}

	/**
	 * Delay to wait after {@code attempt} failed attempts, capped at {@link #maxBackoff()}.
	 *
	 * @param attempt 1-based count of attempts made so far
	 */
	public Duration backoffAfter(int attempt) {
		if (attempt < 1) {
			return Duration.ZERO;
		}
		double scaled = this.initialBackoff.toMillis() * Math.pow(this.multiplier, attempt - 1.0);
		// Math.pow overflows to Infinity for large attempt counts; clamping through double
		// comparison rather than long arithmetic keeps that from wrapping to a negative delay.
		long cappedMillis = (long) Math.min(scaled, (double) this.maxBackoff.toMillis());
		return Duration.ofMillis(Math.max(0, cappedMillis));
	}

	/**
	 * Worst-case total time a single record can occupy a worker thread. The container compares
	 * this against max.poll.interval.ms at startup and warns if retries could outlast the poll
	 * deadline.
	 */
	public Duration worstCaseDuration() {
		Duration total = Duration.ZERO;
		for (int attempt = 1; attempt < this.maxAttempts; attempt++) {
			total = total.plus(backoffAfter(attempt));
		}
		return total;
	}

}
