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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.deserializer.BigQueryDeserializationSchema;
import org.apache.avro.generic.GenericRecord;

import java.util.Optional;

/**
 * Bridge between the Flink version independent connector code (the {@code
 * flink-connector-bigquery-core} module) and the APIs that differ between the supported Flink
 * versions.
 *
 * <p>The core module is compiled against the oldest supported Flink version, so it can only use
 * Flink APIs that are binary compatible across all supported versions. Everything else is reached
 * through this interface. Each Flink version specific connector module ships exactly one
 * implementation, registered through {@code
 * META-INF/services/com.google.cloud.flink.bigquery.compat.FlinkCompat}.
 */
@Internal
public interface FlinkCompat {

    /** Returns the {@link FlinkCompat} implementation bundled with the connector. */
    static FlinkCompat get() {
        return FlinkCompatLoader.get();
    }

    /**
     * Returns whether this Flink version supports the INDIRECT write mode (GCS files + BigQuery
     * load jobs). Callers must check it before using any other indirect write method.
     */
    boolean supportsIndirectWrites();

    /** Returns whether this Flink version supports a custom source parallelism in the Table API. */
    boolean supportsSourceParallelism();

    /**
     * Validates that the restart strategy configured in the job's execution environment is suitable
     * for the exactly-once sink.
     *
     * @throws IllegalArgumentException if the restart strategy is invalid or cannot be validated.
     */
    void validateRestartStrategy(StreamExecutionEnvironment env);

    /** Creates the at-least-once sink, backed by BigQuery's default write stream. */
    <IN> Sink<IN> createDefaultSink(BigQuerySinkConfig<IN> sinkConfig);

    /** Creates the exactly-once sink, backed by BigQuery's buffered write streams. */
    <IN> Sink<IN> createExactlyOnceSink(BigQuerySinkConfig<IN> sinkConfig);

    /**
     * Creates the sink that stages files in GCS and loads them with BigQuery load jobs.
     *
     * @throws UnsupportedOperationException if {@link #supportsIndirectWrites()} is false.
     */
    <IN> Sink<IN> createIndirectSink(BigQuerySinkConfig<IN> sinkConfig);

    /**
     * Creates the bulk writer used by the Table API to stage {@link RowData} files for indirect
     * writes.
     *
     * @throws UnsupportedOperationException if {@link #supportsIndirectWrites()} is false.
     */
    BulkWriter.Factory<RowData> createIndirectBulkWriterFactory(RowType rowType);

    /**
     * Returns the BigQuery load job format matching {@link #createIndirectBulkWriterFactory}.
     *
     * @throws UnsupportedOperationException if {@link #supportsIndirectWrites()} is false.
     */
    FormatOptions getIndirectFormatOptions();

    /**
     * Creates the Table API runtime provider that reads through a {@code BigQuerySource}.
     *
     * @param parallelism The source parallelism requested by the table options, or null. Must be
     *     null if {@link #supportsSourceParallelism()} is false.
     */
    ScanRuntimeProvider createScanRuntimeProvider(
            BigQueryReadOptions readOptions,
            BigQueryDeserializationSchema<GenericRecord, RowData> deserializationSchema,
            Integer parallelism);

    /**
     * Returns the dotted path (e.g. {@code address.city}) of a nested field reference used in a
     * filter, or empty if the expression is not a nested field reference or this Flink version does
     * not support filter push down on nested fields.
     */
    Optional<String> getNestedFieldPath(Expression expression);

    /**
     * Returns a non-nullable RAW logical type serialized with Kryo, used for Avro unions that have
     * no Flink equivalent.
     */
    LogicalType createGenericObjectRawType();
}
