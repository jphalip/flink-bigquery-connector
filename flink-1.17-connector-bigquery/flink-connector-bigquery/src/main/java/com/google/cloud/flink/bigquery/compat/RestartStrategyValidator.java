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

import org.apache.flink.api.common.restartstrategy.RestartStrategies.ExponentialDelayRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FailureRateRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FixedDelayRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates the restart strategy of jobs using the exactly-once sink, based on Flink 1.17's {@link
 * RestartStrategyConfiguration}.
 */
final class RestartStrategyValidator {

    private static final Logger LOG = LoggerFactory.getLogger(RestartStrategyValidator.class);

    private static final long MILLISECONDS_PER_SECOND = 1000L;
    private static final long MILLISECONDS_PER_MINUTE = 60L * 1000L;
    private static final long MILLISECONDS_PER_HOUR = 60L * 60L * 1000L;

    private RestartStrategyValidator() {}

    static void validate(RestartStrategyConfiguration restartStrategy) {
        if (restartStrategy == null) {
            throw new IllegalArgumentException(
                    "Could not read RestartStrategyConfiguration from StreamExecutionEnvironment."
                            + " Please provide the StreamExecutionEnvironment used in Flink job and"
                            + " set a restart strategy.");
        }
        // Restart configurations are mostly being checked against Flink defaults for optimum
        // interactions with external systems, primarily BigQuery storage write APIs.
        // Keep in mind, maximum 10,000 CreateWriteStream calls are allowed by BigQuery per hour per
        // project per region.
        if (restartStrategy instanceof FixedDelayRestartStrategyConfiguration) {
            FixedDelayRestartStrategyConfiguration strategy =
                    (FixedDelayRestartStrategyConfiguration) restartStrategy;
            if (strategy.getDelayBetweenAttemptsInterval().toMilliseconds()
                            < MILLISECONDS_PER_SECOND
                    || strategy.getRestartAttempts() > 10) {
                LOG.error(
                        "Invalid FixedDelayRestartStrategyConfiguration: found restart delay {},"
                                + " milliseconds, and {} restart attempts. Should be used with"
                                + " restart delay at least 1 second, and at most 10 restart"
                                + " attempts.",
                        strategy.getDelayBetweenAttemptsInterval().toMilliseconds(),
                        strategy.getRestartAttempts());
                throw new IllegalArgumentException(
                        "Invalid restart strategy: FixedDelayRestartStrategyConfiguration should"
                                + " be used with at least restart delay 1 second, and at most 10"
                                + " restart attempts.");
            }
        } else if (restartStrategy instanceof ExponentialDelayRestartStrategyConfiguration) {
            ExponentialDelayRestartStrategyConfiguration strategy =
                    (ExponentialDelayRestartStrategyConfiguration) restartStrategy;
            if (strategy.getBackoffMultiplier() < 2.0
                    || strategy.getInitialBackoff().toMilliseconds() < MILLISECONDS_PER_SECOND
                    || strategy.getMaxBackoff().toMilliseconds() < (5L * MILLISECONDS_PER_MINUTE)
                    || strategy.getResetBackoffThreshold().toMilliseconds()
                            < MILLISECONDS_PER_HOUR) {
                LOG.error(
                        "Invalid ExponentialDelayRestartStrategyConfiguration: found backoff"
                                + " multiplier {}, initial backoff {} milliseconds, maximum backoff"
                                + " {} milliseconds, and reset threshold {} milliseconds. Should be"
                                + " used with backoff multiplier at least 2, initial backoff at"
                                + " least 1 second, maximum backoff at least 5 minutes, and reset"
                                + " threshold at least 1 hour",
                        strategy.getBackoffMultiplier(),
                        strategy.getInitialBackoff().toMilliseconds(),
                        strategy.getMaxBackoff().toMilliseconds(),
                        strategy.getResetBackoffThreshold().toMilliseconds());
                throw new IllegalArgumentException(
                        "Invalid restart strategy: ExponentialDelayRestartStrategyConfiguration"
                                + " should be used with backoff multiplier at least 2, initial"
                                + " backoff at least 1 second, maximum backoff at-least 5 minutes,"
                                + " and reset threshold at least 1 hour");
            }
        } else if (restartStrategy instanceof FailureRateRestartStrategyConfiguration) {
            FailureRateRestartStrategyConfiguration strategy =
                    (FailureRateRestartStrategyConfiguration) restartStrategy;
            double failureIntervalInMinutes =
                    ((double) strategy.getFailureInterval().toMilliseconds())
                            / MILLISECONDS_PER_MINUTE;
            double allowedFailuresPerMinute =
                    ((double) strategy.getMaxFailureRate()) / failureIntervalInMinutes;
            if (strategy.getDelayBetweenAttemptsInterval().toMilliseconds()
                            < MILLISECONDS_PER_SECOND
                    || allowedFailuresPerMinute > 1.0) {
                LOG.error(
                        "Invalid FailureRateRestartStrategyConfiguration: found restart delay {}"
                                + " milliseconds, and allowed failure rate {} per minute. Should be"
                                + " used with restart delay at least 1 second, and allowed failure"
                                + " rate at most 1 per minute.",
                        strategy.getDelayBetweenAttemptsInterval().toMilliseconds(),
                        allowedFailuresPerMinute);
                throw new IllegalArgumentException(
                        "Invalid restart strategy: FailureRateRestartStrategyConfiguration should"
                                + " be used with restart delay at least 1 second, and allowed"
                                + " failure rate at most 1 per minute.");
            }
        } else if (restartStrategy instanceof NoRestartStrategyConfiguration) {
            // This is fine!
            LOG.debug("Found NoRestartStrategyConfiguration. No validation needed.");
        } else {
            // Can be either FallbackRestartStrategyConfiguration, or a custom implementation. We
            // don't know how to verify these two possibilities, so we let them pass. If you,
            // worthy reader have a suggestion for validating these scenarios, then kindly share
            // with us at
            // https://github.com/GoogleCloudDataproc/flink-bigquery-connector/issues/new
            // Thanks!
            LOG.warn(
                    "Cannot validate RestartStrategyConfiguration in StreamExecutionEnvironment."
                            + " We recommend explicitly setting the restart strategy as one of the"
                            + " following:"
                            + " FixedDelayRestartStrategyConfiguration,"
                            + " ExponentialDelayRestartStrategyConfiguration,"
                            + " FailureRateRestartStrategyConfiguration or"
                            + " NoRestartStrategyConfiguration");
        }
    }
}
