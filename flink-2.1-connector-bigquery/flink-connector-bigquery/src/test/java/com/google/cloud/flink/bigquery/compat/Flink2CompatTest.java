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
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.BigQueryDefaultSink;
import com.google.cloud.flink.bigquery.sink.BigQueryExactlyOnceSink;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.indirect.RowDataParquetWriterFactory;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import org.junit.Test;

import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.noRestartStrategyConfig;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link Flink2Compat}. */
public class Flink2CompatTest {

    @Test
    public void testLoadsCompatImplementation() {
        assertThat(FlinkCompat.get()).isInstanceOf(Flink2Compat.class);
    }

    @Test
    public void testCapabilities() {
        assertTrue(FlinkCompat.get().supportsIndirectWrites());
        assertTrue(FlinkCompat.get().supportsSourceParallelism());
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
    public void testNestedFieldPath() {
        assertThat(
                        FlinkCompat.get()
                                .getNestedFieldPath(
                                        new NestedFieldReferenceExpression(
                                                new String[] {"address", "city"},
                                                new int[] {1, 0},
                                                DataTypes.STRING())))
                .hasValue("address.city");
        assertThat(
                        FlinkCompat.get()
                                .getNestedFieldPath(
                                        new FieldReferenceExpression(
                                                "city", DataTypes.STRING(), 0, 0)))
                .isEmpty();
    }

    @Test
    public void testGenericObjectRawType() {
        LogicalType type = FlinkCompat.get().createGenericObjectRawType();
        assertThat(type).isInstanceOf(TypeInformationRawType.class);
        assertFalse(type.isNullable());
    }

    @Test
    public void testIndirectWriteFormat() {
        assertThat(FlinkCompat.get().createIndirectBulkWriterFactory(RowType.of(new BigIntType())))
                .isInstanceOf(RowDataParquetWriterFactory.class);
        assertThat(FlinkCompat.get().getIndirectFormatOptions())
                .isEqualTo(RowDataParquetWriterFactory.PARQUET_FORMAT_OPTIONS);
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
