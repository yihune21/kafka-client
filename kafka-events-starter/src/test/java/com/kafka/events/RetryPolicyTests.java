package com.kafka.events;

import java.time.Duration;

import com.kafka.events.consumer.RetryPolicy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class RetryPolicyTests {

	@Test
	void backoffGrowsByTheMultiplier() {
		RetryPolicy policy = new RetryPolicy(5, Duration.ofMillis(100), 2.0, Duration.ofSeconds(30));

		assertThat(policy.backoffAfter(1)).isEqualTo(Duration.ofMillis(100));
		assertThat(policy.backoffAfter(2)).isEqualTo(Duration.ofMillis(200));
		assertThat(policy.backoffAfter(3)).isEqualTo(Duration.ofMillis(400));
	}

	@Test
	void backoffIsCappedAtMaxBackoff() {
		RetryPolicy policy = new RetryPolicy(50, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10));

		assertThat(policy.backoffAfter(10)).isEqualTo(Duration.ofSeconds(10));
	}

	@Test
	void largeAttemptCountsDoNotOverflowIntoANegativeDelay() {
		// Math.pow runs to Infinity long before this; clamping has to happen in double space,
		// because casting Infinity to long first would wrap and hand back a negative Duration.
		RetryPolicy policy = new RetryPolicy(Integer.MAX_VALUE, Duration.ofSeconds(1), 3.0, Duration.ofSeconds(30));

		assertThat(policy.backoffAfter(1000)).isEqualTo(Duration.ofSeconds(30));
		assertThat(policy.backoffAfter(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(30));
	}

	@Test
	void noneRetriesOnlyOnce() {
		assertThat(RetryPolicy.none().maxAttempts()).isEqualTo(1);
		assertThat(RetryPolicy.none().worstCaseDuration()).isZero();
	}

	@Test
	void worstCaseSumsEveryBackoffBetweenAttempts() {
		RetryPolicy policy = new RetryPolicy(3, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30));

		// Three attempts means two waits: 1s then 2s.
		assertThat(policy.worstCaseDuration()).isEqualTo(Duration.ofSeconds(3));
	}

	@Test
	void rejectsNonsensicalSettings() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> new RetryPolicy(0, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(5)));
		assertThatIllegalArgumentException()
			.isThrownBy(() -> new RetryPolicy(3, Duration.ofSeconds(1), 0.5, Duration.ofSeconds(5)));
	}

}
