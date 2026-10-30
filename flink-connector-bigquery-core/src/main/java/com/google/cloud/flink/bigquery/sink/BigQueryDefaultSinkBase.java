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
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.cloud.flink.bigquery.services.BigQueryServicesImpl;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryDefaultWriter;

/**
 * Flink version independent part of the sink that writes data into a BigQuery table using {@link
 * BigQueryDefaultWriter}. The {@code BigQueryDefaultSink} in each Flink version specific connector
 * module adapts it to that version's {@code Sink} API.
 *
 * @param <IN> Type of input to sink.
 */
@Internal
public abstract class BigQueryDefaultSinkBase<IN> extends BigQueryBaseSink<IN> {

    protected BigQueryDefaultSinkBase(BigQuerySinkConfig<IN> sinkConfig) {
        super(sinkConfig);
        traceId = BigQueryServicesImpl.generateTraceId("default");
    }

    protected BigQueryDefaultWriter<IN> createDefaultWriter(
            int subtaskId, int numberOfParallelSubtasks, SinkWriterMetricGroup metricGroup) {
        checkParallelism(numberOfParallelSubtasks);
        return new BigQueryDefaultWriter<>(
                tablePath,
                connectOptions,
                schemaProvider,
                serializer,
                createTableOptions(),
                fatalizeSerializer,
                maxParallelism,
                traceId,
                cdcEnabled,
                cdcSequenceField,
                cdcChangeTypeProvider,
                subtaskId,
                metricGroup);
    }
}
