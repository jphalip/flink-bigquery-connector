/*
 * Copyright (C) 2024 Google Inc.
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
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import com.google.cloud.flink.bigquery.lineage.BigQueryLineageUtil;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryBufferedWriter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;
import com.google.cloud.flink.bigquery.sink.writer.PrecommittingBufferedWriter;

import java.util.Collection;

/**
 * Sink to write data into a BigQuery table using {@link BigQueryBufferedWriter}.
 *
 * <p>Depending on the checkpointing mode, this writer offers the following consistency guarantees:
 * <li>{@link CheckpointingMode#EXACTLY_ONCE}: exactly-once write consistency.
 * <li>{@link CheckpointingMode#AT_LEAST_ONCE}: at-least-once write consistency.
 * <li>Checkpointing disabled (NOT RECOMMENDED!): no consistency guarantee.
 *
 * @param <IN> Type of input to sink.
 */
public class BigQueryExactlyOnceSink<IN> extends BigQueryExactlyOnceSinkBase<IN>
        implements TwoPhaseCommittingStatefulSink<IN, BigQueryWriterState, BigQueryCommittable>,
                LineageVertexProvider {

    @Internal
    public BigQueryExactlyOnceSink(BigQuerySinkConfig<IN> sinkConfig) {
        super(sinkConfig);
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            createWriter(WriterInitContext context) {
        return new PrecommittingBufferedWriter<>(
                createBufferedWriter(
                        context.getTaskInfo().getIndexOfThisSubtask(),
                        context.getTaskInfo().getNumberOfParallelSubtasks(),
                        context.metricGroup()));
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            restoreWriter(
                    WriterInitContext context, Collection<BigQueryWriterState> recoveredState) {
        return new PrecommittingBufferedWriter<>(
                restoreBufferedWriter(
                        context.getTaskInfo().getIndexOfThisSubtask(),
                        context.getTaskInfo().getNumberOfParallelSubtasks(),
                        context.metricGroup(),
                        recoveredState));
    }

    @Override
    public Committer<BigQueryCommittable> createCommitter(CommitterInitContext context) {
        return createBigQueryCommitter();
    }

    /**
     * Exposes this sink's BigQuery table as FLIP-314 lineage metadata, with the {@code (namespace,
     * name)} pair {@code ("bigquery", "project.dataset.table")} a downstream {@link
     * org.apache.flink.core.execution.JobStatusChangedListener} can consume. See {@link
     * BigQueryLineageUtil} for details.
     */
    @Override
    public LineageVertex getLineageVertex() {
        return BigQueryLineageUtil.sinkVertexOf(connectOptions);
    }
}
