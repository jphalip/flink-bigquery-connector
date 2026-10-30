/*
 * Copyright (C) 2023 Google Inc.
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

package com.google.cloud.flink.bigquery.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import com.google.auto.value.AutoValue;
import com.google.cloud.flink.bigquery.lineage.BigQueryLineageUtil;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.deserializer.BigQueryDeserializationSchema;
import org.apache.avro.generic.GenericRecord;

/**
 * The DataStream {@link Source} implementation for Google BigQuery. It reads data directly from a
 * BigQuery table. Only bounded source is supported right now.
 *
 * <p>Review the option classes and their builders for more details on the configurable options.
 *
 * <p>The following example demonstrates how to create a bounded source to read {@link
 * GenericRecord} data from a BigQuery table.
 *
 * <pre>{@code
 * BigQuerySource<GenericRecord> source =
 *       BigQuerySource.readAvros(
 *           BigQueryReadOptions.builder()
 *             .setBigQueryConnectOptions(
 *               BigQueryConnectOptions.builder()
 *                 .setProjectId("some-gcp-project")
 *                 .setDataset("some-bq-dataset")
 *                 .setTable("some-bq-table")
 *                 .build())
 *             .build());
 * }</pre>
 *
 * @param <OUT> The type of the data returned by this source implementation.
 */
@AutoValue
@PublicEvolving
public abstract class BigQuerySource<OUT> extends BigQuerySourceBase<OUT>
        implements LineageVertexProvider {

    /**
     * Exposes this source's BigQuery table as FLIP-314 lineage metadata, with the {@code
     * (namespace, name)} pair {@code ("bigquery", "project.dataset.table")} a downstream {@link
     * org.apache.flink.core.execution.JobStatusChangedListener} can consume. Implemented on the
     * source (not the pipeline) so lineage is emitted for pure FlinkSQL jobs too. See {@link
     * BigQueryLineageUtil} for details.
     */
    @Override
    public LineageVertex getLineageVertex() {
        return BigQueryLineageUtil.sourceVertexOf(
                getReadOptions().getBigQueryConnectOptions(), getBoundedness());
    }

    /**
     * Transforms the instance into a builder instance for property modification.
     *
     * @return A {@link Builder} instance for the type.
     */
    public abstract Builder<OUT> toBuilder();

    /**
     * Creates an instance of this class builder.
     *
     * @param <OUT> The expected return type of this source.
     * @return the BigQuerySource builder instance.
     */
    public static <OUT> Builder<OUT> builder() {
        return new AutoValue_BigQuerySource.Builder<OUT>()
                .setSourceBoundedness(Boundedness.BOUNDED);
    }

    /**
     * Creates an instance of the source, setting Avro {@link GenericRecord} as the return type for
     * the data (mimicking the table's schema). In case of projecting the columns of the table a new
     * de-serialization schema should be provided (considering the new result projected schema).
     *
     * @param readOptions The read options for this source
     * @return A fully initialized instance of the source, ready to read {@link GenericRecord} from
     *     the underlying table.
     */
    public static BigQuerySource<GenericRecord> readAvros(BigQueryReadOptions readOptions) {
        return BigQuerySource.<GenericRecord>builder()
                .setDeserializationSchema(createAvroDeserializationSchema(readOptions))
                .setReadOptions(readOptions)
                .build();
    }

    /**
     * Builder class for {@link BigQuerySource}.
     *
     * @param <OUT> The type of the data returned by this source implementation.
     */
    @AutoValue.Builder
    public abstract static class Builder<OUT> {
        /**
         * Sets the de-serialization schema for BigQuery returned data (AVRO by default).
         *
         * @param deserSchema the de-serialization schema for BigQuery return type.
         * @return the BigQuerySource builder instance.
         */
        public abstract Builder<OUT> setDeserializationSchema(
                BigQueryDeserializationSchema<GenericRecord, OUT> deserSchema);

        /**
         * Sets the BigQuery read options that configures the source's instance.
         *
         * @param options The instance of the BigQuery read options.
         * @return the BigQuerySource builder instance.
         */
        public abstract Builder<OUT> setReadOptions(BigQueryReadOptions options);

        /**
         * Sets how the source will scan and read data from BigQuery, in batch fashion (once from a
         * table, partition or query results) or continuously (reading new completed partitions as
         * they appear).
         *
         * @param boundedness The boundedness of the source
         * @return the BigQuerySource builder instance.
         */
        public abstract Builder<OUT> setSourceBoundedness(Boundedness boundedness);

        /**
         * Creates an instance of the {@link BigQuerySource}.
         *
         * @return A fully initialized instance of the source.
         */
        public abstract BigQuerySource<OUT> build();
    }
}
