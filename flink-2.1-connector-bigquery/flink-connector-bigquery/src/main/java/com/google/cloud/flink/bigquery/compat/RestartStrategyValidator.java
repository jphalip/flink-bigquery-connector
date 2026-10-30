/*
 * Copyright (C) 2026 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.compat;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * Validates the restart strategy of jobs using the exactly-once sink, based on the job's
 * configuration ({@link RestartStrategyOptions}).
 */
final class RestartStrategyValidator {

    private static final Logger LOG = LoggerFactory.getLogger(RestartStrategyValidator.class);

    private static final long MILLISECONDS_PER_SECOND = 1000L;
    private static final long MILLISECONDS_PER_MINUTE = 60L * 1000L;
    private static final long MILLISECONDS_PER_HOUR = 60L * 60L * 1000L;
    private static final String RESTART_STRATEGY_FIXED_DELAY = "fixed-delay";
    private static final String RESTART_STRATEGY_EXPONENTIAL_DELAY = "exponential-delay";
    private static final String RESTART_STRATEGY_FAILURE_RATE = "failure-rate";
    private static final String RESTART_STRATEGY_NONE = "none";

    private RestartStrategyValidator() {}

    static void validate(ReadableConfig config) {
        if (config == null) {
            throw new IllegalArgumentException(
                    "Could not read Configuration from StreamExecutionEnvironment."
                            + " Please provide the StreamExecutionEnvironment used in Flink job and"
                            + " set a restart strategy.");
        }

        Optional<String> maybeRestartStrategy =
                config.getOptional(RestartStrategyOptions.RESTART_STRATEGY);
        if (maybeRestartStrategy.isPresent()) {
            // Restart configurations are mostly being checked against Flink defaults for optimum
            // interactions with external systems, primarily BigQuery storage write APIs.
            // Keep in mind, maximum 10,000 CreateWriteStream calls are allowed by BigQuery per hour
            // per
            // project per region.
            String restartStrategy = maybeRestartStrategy.get();
            if (restartStrategy.equals(RESTART_STRATEGY_FIXED_DELAY)) {
                Duration delayBetweenAttemptsInterval =
                        config.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY);
                Integer restartAttempts =
                        config.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);

                if (delayBetweenAttemptsInterval.toMillis() < MILLISECONDS_PER_SECOND
                        || restartAttempts > 10) {
                    LOG.error(
                            "Invalid fixed delay restart strategy configuration: found restart delay {},"
                                    + " milliseconds, and {} restart attempts. Should be used with"
                                    + " restart delay at least 1 second, and at most 10 restart"
                                    + " attempts.",
                            delayBetweenAttemptsInterval.toMillis(),
                            restartAttempts);
                    throw new IllegalArgumentException(
                            "Invalid restart strategy: fixed delay restart strategy configuration should"
                                    + " be used with at least restart delay 1 second, and at most 10"
                                    + " restart attempts.");
                }
            } else if (restartStrategy.equals(RESTART_STRATEGY_EXPONENTIAL_DELAY)) {
                Double backoffMultiplier =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER);
                Duration initialBackoff =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
                Duration maxBackoff =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF);
                Duration resetBackoffThreshold =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD);

                if (backoffMultiplier < 2.0
                        || initialBackoff.toMillis() < MILLISECONDS_PER_SECOND
                        || maxBackoff.toMillis() < (5L * MILLISECONDS_PER_MINUTE)
                        || resetBackoffThreshold.toMillis() < MILLISECONDS_PER_HOUR) {
                    LOG.error(
                            "Invalid exponential delay restart strategy configuration: found backoff"
                                    + " multiplier {}, initial backoff {} milliseconds, maximum backoff"
                                    + " {} milliseconds, and reset threshold {} milliseconds. Should be"
                                    + " used with backoff multiplier at least 2, initial backoff at"
                                    + " least 1 second, maximum backoff at least 5 minutes, and reset"
                                    + " threshold at least 1 hour",
                            backoffMultiplier,
                            initialBackoff.toMillis(),
                            maxBackoff.toMillis(),
                            resetBackoffThreshold.toMillis());
                    throw new IllegalArgumentException(
                            "Invalid restart strategy: exponential delay restart strategy configuration"
                                    + " should be used with backoff multiplier at least 2, initial"
                                    + " backoff at least 1 second, maximum backoff at-least 5 minutes,"
                                    + " and reset threshold at least 1 hour");
                }
            } else if (restartStrategy.equals(RESTART_STRATEGY_FAILURE_RATE)) {
                Duration failureInterval =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL);
                Integer maxFailureRate =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
                Duration delayBetweenAttemptsInterval =
                        config.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY);

                double failureIntervalInMinutes =
                        ((double) failureInterval.toMillis()) / MILLISECONDS_PER_MINUTE;
                double allowedFailuresPerMinute =
                        ((double) maxFailureRate) / failureIntervalInMinutes;
                if (delayBetweenAttemptsInterval.toMillis() < MILLISECONDS_PER_SECOND
                        || allowedFailuresPerMinute > 1.0) {
                    LOG.error(
                            "Invalid failure rate restart strategy configuration: found restart delay {}"
                                    + " milliseconds, and allowed failure rate {} per minute. Should be"
                                    + " used with restart delay at least 1 second, and allowed failure"
                                    + " rate at most 1 per minute.",
                            delayBetweenAttemptsInterval.toMillis(),
                            allowedFailuresPerMinute);
                    throw new IllegalArgumentException(
                            "Invalid restart strategy: failure rate restart strategy configuration should"
                                    + " be used with restart delay at least 1 second, and allowed"
                                    + " failure rate at most 1 per minute.");
                }
            } else if (restartStrategy.equals(RESTART_STRATEGY_NONE)) {
                LOG.debug("Found no restart strategy. No validation needed.");
            } else {
                throw new IllegalStateException(
                        "Encountered unexpected restart strategy: " + restartStrategy);
            }
        } else {
            throw new IllegalArgumentException(
                    "Cannot validate RestartStrategyConfiguration in StreamExecutionEnvironment."
                            + " We recommend explicitly setting the restart strategy as one of the"
                            + " following:"
                            + " "
                            + RESTART_STRATEGY_FIXED_DELAY
                            + ","
                            + " "
                            + RESTART_STRATEGY_EXPONENTIAL_DELAY
                            + ","
                            + " "
                            + RESTART_STRATEGY_FAILURE_RATE
                            + " or"
                            + " "
                            + RESTART_STRATEGY_NONE);
        }
    }
}
