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

package com.google.cloud.flink.bigquery.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.utils.FactoryMocks;

import com.google.cloud.flink.bigquery.sink.WriteMode;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Tests for the {@link BigQueryDynamicTableFactory} features that require Flink 2.x. */
public class BigQueryDynamicTableFactoryFlink2Test {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Arrays.asList("bbb", "aaa")));

    @Test
    public void testIndirectWriteModeBatchModeCreatesSinkWithIndirectMode() {
        Map<String, String> options = getRequiredOptions();
        options.put(BigQueryConnectorOptions.WRITE_MODE.key(), "INDIRECT");
        options.put(BigQueryConnectorOptions.TEMP_GCS_PATH.key(), "gs://bucket/tmp");

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        DynamicTableSink sink =
                FactoryUtil.createDynamicTableSink(
                        null,
                        FactoryMocks.IDENTIFIER,
                        new ResolvedCatalogTable(
                                CatalogTable.newBuilder()
                                        .schema(
                                                org.apache.flink.table.api.Schema.newBuilder()
                                                        .fromResolvedSchema(SCHEMA)
                                                        .build())
                                        .comment("mock sink")
                                        .options(options)
                                        .build(),
                                SCHEMA),
                        Collections.emptyMap(),
                        config,
                        getClass().getClassLoader(),
                        false);

        assertThat(sink).isInstanceOf(BigQueryDynamicTableSink.class);
        BigQueryDynamicTableSink dynamicSink = (BigQueryDynamicTableSink) sink;
        assertEquals(WriteMode.INDIRECT, dynamicSink.getSinkConfig().getWriteMode());
        assertEquals("gs://bucket/tmp", dynamicSink.getSinkConfig().getTempGcsPath());
    }

    @Test
    public void testDefaultWriteModeIsDirect() {
        DynamicTableSink sink =
                FactoryUtil.createDynamicTableSink(
                        null,
                        FactoryMocks.IDENTIFIER,
                        new ResolvedCatalogTable(
                                CatalogTable.newBuilder()
                                        .schema(
                                                org.apache.flink.table.api.Schema.newBuilder()
                                                        .fromResolvedSchema(SCHEMA)
                                                        .build())
                                        .comment("mock sink")
                                        .options(getRequiredOptions())
                                        .build(),
                                SCHEMA),
                        Collections.emptyMap(),
                        new Configuration(),
                        getClass().getClassLoader(),
                        false);

        assertThat(sink).isInstanceOf(BigQueryDynamicTableSink.class);
        BigQueryDynamicTableSink dynamicSink = (BigQueryDynamicTableSink) sink;
        assertEquals(WriteMode.STORAGE_WRITE_API, dynamicSink.getSinkConfig().getWriteMode());
        assertNull(dynamicSink.getSinkConfig().getTempGcsPath());
    }

    @Test
    public void testBigQuerySourceWithParallelism() throws IOException {
        Map<String, String> properties = getRequiredOptions();
        Integer sourceParallelism = 100;
        properties.put(
                BigQueryConnectorOptions.SOURCE_PARALLELISM.key(),
                String.valueOf(sourceParallelism));

        DynamicTableSource actual = FactoryMocks.createTableSource(SCHEMA, properties);

        assertThat(actual).isInstanceOf(BigQueryDynamicTableSource.class);
        assertEquals(
                sourceParallelism, ((BigQueryDynamicTableSource) actual).getSourceParallelism());
    }

    private static Map<String, String> getRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "bigquery");
        options.put(BigQueryConnectorOptions.PROJECT.key(), "project");
        options.put(BigQueryConnectorOptions.DATASET.key(), "dataset");
        options.put(BigQueryConnectorOptions.TABLE.key(), "table");
        return options;
    }
}
