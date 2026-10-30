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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TypeInformationRawType;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.flink.bigquery.sink.BigQueryDefaultSink;
import com.google.cloud.flink.bigquery.sink.BigQueryExactlyOnceSink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.deserializer.BigQueryDeserializationSchema;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import org.apache.avro.generic.GenericRecord;

import java.util.Optional;

/** {@link FlinkCompat} implementation for Flink 1.17. */
@Internal
public final class Flink117Compat implements FlinkCompat {

    private static final String INDIRECT_WRITES_NOT_SUPPORTED =
            "INDIRECT write mode is not supported with Flink 1.17. Use the BigQuery connector for"
                    + " Flink 2.x.";

    @Override
    public boolean supportsIndirectWrites() {
        return false;
    }

    @Override
    public boolean supportsSourceParallelism() {
        return false;
    }

    @Override
    public void validateRestartStrategy(StreamExecutionEnvironment env) {
        RestartStrategyValidator.validate(env.getRestartStrategy());
    }

    @Override
    public <IN> Sink<IN> createDefaultSink(BigQuerySinkConfig<IN> sinkConfig) {
        return new BigQueryDefaultSink<>(sinkConfig);
    }

    @Override
    public <IN> Sink<IN> createExactlyOnceSink(BigQuerySinkConfig<IN> sinkConfig) {
        return new BigQueryExactlyOnceSink<>(sinkConfig);
    }

    @Override
    public <IN> Sink<IN> createIndirectSink(BigQuerySinkConfig<IN> sinkConfig) {
        throw new UnsupportedOperationException(INDIRECT_WRITES_NOT_SUPPORTED);
    }

    @Override
    public BulkWriter.Factory<RowData> createIndirectBulkWriterFactory(RowType rowType) {
        throw new UnsupportedOperationException(INDIRECT_WRITES_NOT_SUPPORTED);
    }

    @Override
    public FormatOptions getIndirectFormatOptions() {
        throw new UnsupportedOperationException(INDIRECT_WRITES_NOT_SUPPORTED);
    }

    @Override
    public ScanRuntimeProvider createScanRuntimeProvider(
            BigQueryReadOptions readOptions,
            BigQueryDeserializationSchema<GenericRecord, RowData> deserializationSchema,
            Integer parallelism) {
        if (parallelism != null) {
            throw new ValidationException(
                    String.format(
                            "The '%s' option is not supported with Flink 1.17.",
                            BigQueryConnectorOptions.SOURCE_PARALLELISM.key()));
        }
        return SourceProvider.of(
                BigQuerySource.<RowData>builder()
                        .setReadOptions(readOptions)
                        .setDeserializationSchema(deserializationSchema)
                        .build());
    }

    @Override
    public Optional<String> getNestedFieldPath(Expression expression) {
        // Flink 1.17 does not push down filters on nested fields.
        return Optional.empty();
    }

    @Override
    public LogicalType createGenericObjectRawType() {
        return new TypeInformationRawType<>(false, Types.GENERIC(Object.class));
    }
}
