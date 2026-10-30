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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.cloud.flink.bigquery.services.BigQueryServicesImpl;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittableSerializer;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommitter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryBufferedWriter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterStateSerializer;

import java.util.Collection;
import java.util.Comparator;
import java.util.UUID;

/**
 * Flink version independent part of the sink that writes data into a BigQuery table using {@link
 * BigQueryBufferedWriter} and {@link BigQueryCommitter}. The {@code BigQueryExactlyOnceSink} in
 * each Flink version specific connector module adapts it to that version's two-phase committing and
 * stateful {@code Sink} APIs.
 *
 * @param <IN> Type of input to sink.
 */
@Internal
public abstract class BigQueryExactlyOnceSinkBase<IN> extends BigQueryBaseSink<IN> {

    protected BigQueryExactlyOnceSinkBase(BigQuerySinkConfig<IN> sinkConfig) {
        super(sinkConfig);
        traceId = BigQueryServicesImpl.generateTraceId(UUID.randomUUID().toString());
    }

    protected BigQueryBufferedWriter<IN> createBufferedWriter(
            int subtaskId, int numberOfParallelSubtasks, SinkWriterMetricGroup metricGroup) {
        checkParallelism(numberOfParallelSubtasks);
        return new BigQueryBufferedWriter<>(
                tablePath,
                connectOptions,
                schemaProvider,
                serializer,
                createTableOptions(),
                fatalizeSerializer,
                maxParallelism,
                traceId,
                subtaskId,
                metricGroup);
    }

    protected BigQueryBufferedWriter<IN> restoreBufferedWriter(
            int subtaskId,
            int numberOfParallelSubtasks,
            SinkWriterMetricGroup metricGroup,
            Collection<BigQueryWriterState> recoveredState) {
        if (recoveredState == null || recoveredState.isEmpty()) {
            return createBufferedWriter(subtaskId, numberOfParallelSubtasks, metricGroup);
        }
        // If multiple states are found, restore one with the latest checkpoint.
        BigQueryWriterState stateToRestore =
                recoveredState.stream()
                        .max(Comparator.comparingLong(state -> state.getCheckpointId()))
                        .get();
        return new BigQueryBufferedWriter<>(
                stateToRestore.getStreamName(),
                stateToRestore.getStreamOffset(),
                tablePath,
                stateToRestore.getTotalRecordsSeen(),
                stateToRestore.getTotalRecordsWritten(),
                stateToRestore.getTotalRecordsCommitted(),
                connectOptions,
                schemaProvider,
                serializer,
                createTableOptions(),
                fatalizeSerializer,
                maxParallelism,
                traceId,
                subtaskId,
                metricGroup);
    }

    protected BigQueryCommitter createBigQueryCommitter() {
        return new BigQueryCommitter(connectOptions);
    }

    public SimpleVersionedSerializer<BigQueryCommittable> getCommittableSerializer() {
        return new BigQueryCommittableSerializer();
    }

    public SimpleVersionedSerializer<BigQueryWriterState> getWriterStateSerializer() {
        return new BigQueryWriterStateSerializer();
    }
}
