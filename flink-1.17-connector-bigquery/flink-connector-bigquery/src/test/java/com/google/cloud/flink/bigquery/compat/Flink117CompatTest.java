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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.util.ExceptionUtils;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.BigQueryDefaultSink;
import com.google.cloud.flink.bigquery.sink.BigQueryExactlyOnceSink;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.WriteMode;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.noRestartStrategyConfig;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tests for {@link Flink117Compat} and for the features that Flink 1.17 does not support. */
public class Flink117CompatTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(Column.physical("number", DataTypes.BIGINT()));

    @Test
    public void testLoadsCompatImplementation() {
        assertThat(FlinkCompat.get()).isInstanceOf(Flink117Compat.class);
    }

    @Test
    public void testCapabilities() {
        assertFalse(FlinkCompat.get().supportsIndirectWrites());
        assertFalse(FlinkCompat.get().supportsSourceParallelism());
    }

    @Test
    public void testSinkForAtLeastOnce() throws Exception {
        assertThat(BigQuerySink.get(sinkConfig(DeliveryGuarantee.AT_LEAST_ONCE)))
                .isInstanceOf(BigQueryDefaultSink.class);
    }

    @Test
    public void testSinkForExactlyOnce() throws Exception {
        assertThat(BigQuerySink.get(sinkConfig(DeliveryGuarantee.EXACTLY_ONCE)))
                .isInstanceOf(BigQueryExactlyOnceSink.class);
    }

    @Test
    public void testIndirectWriteModeNotSupported() {
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<Object>newBuilder()
                                        .connectOptions(
                                                StorageClientFaker.createConnectOptionsForWrite(
                                                        null))
                                        .writeMode(WriteMode.INDIRECT)
                                        .build());
        assertThat(exception)
                .hasMessageThat()
                .contains("INDIRECT write mode is not supported with Flink 1.17");
    }

    @Test
    public void testTableIndirectWriteModeNotSupported() {
        Map<String, String> options = requiredTableOptions();
        options.put(BigQueryConnectorOptions.WRITE_MODE.key(), "INDIRECT");
        ValidationException exception =
                assertThrows(
                        ValidationException.class,
                        () -> FactoryMocks.createTableSink(SCHEMA, options));
        assertTrue(
                ExceptionUtils.findThrowableWithMessage(
                                exception, "'write.mode'='INDIRECT' is not supported")
                        .isPresent());
    }

    @Test
    public void testTableSourceParallelismNotSupported() {
        Map<String, String> options = requiredTableOptions();
        options.put(BigQueryConnectorOptions.SOURCE_PARALLELISM.key(), "10");
        ValidationException exception =
                assertThrows(
                        ValidationException.class,
                        () -> FactoryMocks.createTableSource(SCHEMA, options));
        assertTrue(
                ExceptionUtils.findThrowableWithMessage(
                                exception, "'scan.parallelism' is not supported")
                        .isPresent());
    }

    @Test
    public void testIndirectWriteMethodsNotSupported() {
        FlinkCompat compat = FlinkCompat.get();
        assertThrows(UnsupportedOperationException.class, () -> compat.createIndirectSink(null));
        assertThrows(
                UnsupportedOperationException.class,
                () -> compat.createIndirectBulkWriterFactory(null));
        assertThrows(UnsupportedOperationException.class, compat::getIndirectFormatOptions);
    }

    @Test
    public void testScanRuntimeProviderRejectsParallelism() {
        assertThrows(
                ValidationException.class,
                () -> FlinkCompat.get().createScanRuntimeProvider(null, null, 10));
    }

    @Test
    public void testNoNestedFieldPushDown() {
        assertFalse(
                FlinkCompat.get()
                        .getNestedFieldPath(
                                new FieldReferenceExpression("f", DataTypes.INT(), 0, 0))
                        .isPresent());
    }

    @Test
    public void testGenericObjectRawType() {
        LogicalType type = FlinkCompat.get().createGenericObjectRawType();
        assertThat(type).isInstanceOf(TypeInformationRawType.class);
        assertFalse(type.isNullable());
    }

    private static Map<String, String> requiredTableOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "bigquery");
        options.put(BigQueryConnectorOptions.PROJECT.key(), "project");
        options.put(BigQueryConnectorOptions.DATASET.key(), "dataset");
        options.put(BigQueryConnectorOptions.TABLE.key(), "table");
        return options;
    }

    private static BigQuerySinkConfig<Object> sinkConfig(DeliveryGuarantee deliveryGuarantee)
            throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(noRestartStrategyConfig())) {
            return BigQuerySinkConfig.<Object>newBuilder()
                    .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                    .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                    .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                    .deliveryGuarantee(deliveryGuarantee)
                    .streamExecutionEnvironment(env)
                    .build();
        }
    }
}
