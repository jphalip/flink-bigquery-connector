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

package com.google.cloud.flink.bigquery.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.emitter.BigQueryRecordEmitter;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumStateSerializer;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumerator;
import com.google.cloud.flink.bigquery.source.reader.BigQuerySourceReader;
import com.google.cloud.flink.bigquery.source.reader.BigQuerySourceReaderContext;
import com.google.cloud.flink.bigquery.source.reader.deserializer.AvroDeserializationSchema;
import com.google.cloud.flink.bigquery.source.reader.deserializer.BigQueryDeserializationSchema;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplitSerializer;
import com.google.cloud.flink.bigquery.source.split.reader.BigQuerySourceSplitReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Flink version independent implementation of the DataStream {@link Source} for Google BigQuery.
 *
 * <p>The public {@code BigQuerySource} class, which lives in each Flink version specific connector
 * module, extends this class with its builder and the interfaces that only exist in some Flink
 * versions.
 *
 * @param <OUT> The type of the data returned by this source implementation.
 */
@Internal
public abstract class BigQuerySourceBase<OUT>
        implements Source<OUT, BigQuerySourceSplit, BigQuerySourceEnumState>,
                ResultTypeQueryable<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceBase.class);

    public abstract BigQueryDeserializationSchema<GenericRecord, OUT> getDeserializationSchema();

    public abstract BigQueryReadOptions getReadOptions();

    public abstract Boundedness getSourceBoundedness();

    @Override
    public Boundedness getBoundedness() {
        return getSourceBoundedness();
    }

    @Override
    public SimpleVersionedSerializer<BigQuerySourceSplit> getSplitSerializer() {
        return BigQuerySourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<BigQuerySourceEnumState> getEnumeratorCheckpointSerializer() {
        return BigQuerySourceEnumStateSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return getDeserializationSchema().getProducedType();
    }

    @Override
    public SourceReader<OUT, BigQuerySourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        BigQuerySourceReaderContext bqReaderContext =
                new BigQuerySourceReaderContext(
                        readerContext, getReadOptions().getLimit().orElse(-1));

        Supplier<SplitReader<GenericRecord, BigQuerySourceSplit>> splitReaderSupplier =
                () -> new BigQuerySourceSplitReader(getReadOptions(), bqReaderContext);

        return new BigQuerySourceReader<>(
                splitReaderSupplier,
                new BigQueryRecordEmitter<>(getDeserializationSchema()),
                bqReaderContext);
    }

    @Override
    public SplitEnumerator<BigQuerySourceSplit, BigQuerySourceEnumState> createEnumerator(
            SplitEnumeratorContext<BigQuerySourceSplit> enumContext) throws Exception {
        return new BigQuerySourceEnumerator(
                getBoundedness(),
                enumContext,
                getReadOptions(),
                BigQuerySourceEnumState.initialState());
    }

    @Override
    public SplitEnumerator<BigQuerySourceSplit, BigQuerySourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<BigQuerySourceSplit> enumContext,
            BigQuerySourceEnumState checkpoint)
            throws Exception {
        LOG.debug("Restoring enumerator with state {}", checkpoint);
        return new BigQuerySourceEnumerator(
                getBoundedness(), enumContext, getReadOptions(), checkpoint);
    }

    /**
     * Creates the de-serialization schema that returns Avro {@link GenericRecord}s mimicking the
     * schema of the table configured in the read options.
     *
     * @param readOptions The read options of the source.
     * @return The de-serialization schema for the table's records.
     */
    protected static AvroDeserializationSchema createAvroDeserializationSchema(
            BigQueryReadOptions readOptions) {
        BigQueryConnectOptions connectOptions = readOptions.getBigQueryConnectOptions();
        TableSchema tableSchema =
                BigQueryServicesFactory.instance(connectOptions)
                        .queryClient()
                        .getTableSchema(
                                connectOptions.getProjectId(),
                                connectOptions.getDataset(),
                                connectOptions.getTable());
        return new AvroDeserializationSchema(
                SchemaTransform.toGenericAvroSchema(
                                String.format(
                                        "%s.%s.%s",
                                        sanitizeAvroSchemaName(connectOptions.getProjectId()),
                                        sanitizeAvroSchemaName(connectOptions.getDataset()),
                                        sanitizeAvroSchemaName(connectOptions.getTable())),
                                tableSchema.getFields())
                        .toString());
    }

    /** Replaces invalid characters with underscore. */
    private static String sanitizeAvroSchemaName(String name) {
        if (name == null) {
            return name;
        }
        int length = name.length();
        if (length == 0) {
            return name;
        }
        String newName = "";
        char first = name.charAt(0);
        if (Character.isLetter(first) || first == '_') {
            newName += first;
        } else {
            newName += '_';
        }
        for (int i = 1; i < length; i++) {
            char c = name.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') {
                newName += c;
            } else {
                newName += '_';
            }
        }
        return newName;
    }
}
