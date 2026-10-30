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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.indirect.RowDataParquetWriterFactory;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BigQuerySinkConfig} that depend on the INDIRECT write implementation. */
public class BigQuerySinkConfigIndirectTest {

    private static final String GCS_PATH = "gs://bucket/tmp";
    private static final String TEMP_PROJECT = "temp_p";
    private static final String TEMP_DATASET = "temp_d";
    private static final String JOB_PROJECT = "job_p";

    private static BigQueryConnectOptions indirectConnectOptions() {
        return BigQueryConnectOptions.builder()
                .setProjectId("p")
                .setDataset("d")
                .setTable("t")
                .build();
    }

    private static BulkWriter.Factory<String> dummyBulkWriterFactory() {
        return new DummyBulkWriterFactory();
    }

    @Test
    public void equalsMethodHandlesBulkWriterFactoryCorrectly() {
        RowType rowType = RowType.of(new BigIntType());
        BigQuerySinkConfig<RowData> a =
                BigQuerySinkConfig.<RowData>newBuilder()
                        .writeMode(WriteMode.INDIRECT)
                        .connectOptions(indirectConnectOptions())
                        .tempGcsPath(GCS_PATH)
                        .tempProject(TEMP_PROJECT)
                        .tempDataset(TEMP_DATASET)
                        .jobProject(JOB_PROJECT)
                        .bulkWriterFactory(RowDataParquetWriterFactory.create(rowType))
                        .formatOptions(FormatOptions.parquet())
                        .build();
        BigQuerySinkConfig<RowData> b =
                BigQuerySinkConfig.<RowData>newBuilder()
                        .writeMode(WriteMode.INDIRECT)
                        .connectOptions(indirectConnectOptions())
                        .tempGcsPath(GCS_PATH)
                        .tempProject(TEMP_PROJECT)
                        .tempDataset(TEMP_DATASET)
                        .jobProject(JOB_PROJECT)
                        .bulkWriterFactory(RowDataParquetWriterFactory.create(rowType))
                        .formatOptions(FormatOptions.parquet())
                        .build();

        assertNotSame(a.getBulkWriterFactory(), b.getBulkWriterFactory());

        assertEquals(a, b);
    }

    @Test
    public void buildIndirectHappyPathReturnsConfigWithGetters() {
        BigQueryConnectOptions opts = indirectConnectOptions();
        BulkWriter.Factory<String> factory = dummyBulkWriterFactory();

        BigQuerySinkConfig<String> config =
                BigQuerySinkConfig.<String>newBuilder()
                        .writeMode(WriteMode.INDIRECT)
                        .connectOptions(opts)
                        .tempGcsPath(GCS_PATH)
                        .tempProject(TEMP_PROJECT)
                        .tempDataset(TEMP_DATASET)
                        .jobProject(JOB_PROJECT)
                        .bulkWriterFactory(factory)
                        .formatOptions(FormatOptions.parquet())
                        .build();

        assertSame(opts, config.getConnectOptions());
        assertEquals(GCS_PATH, config.getTempGcsPath());
        assertEquals(TEMP_PROJECT, config.getTempProject());
        assertEquals(TEMP_DATASET, config.getTempDataset());
        assertEquals(JOB_PROJECT, config.getJobProject());
        assertSame(factory, config.getBulkWriterFactory());
        assertEquals(FormatOptions.parquet(), config.getFormatOptions());
        assertEquals(WriteMode.INDIRECT, config.getWriteMode());
    }

    @Test
    public void buildIndirectNullTempDatasetThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempProject(TEMP_PROJECT)
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .build());
        assertTrue(ex.getMessage().contains("tempDataset is required"));
    }

    @Test
    public void buildIndirectEmptyTempDatasetThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempProject(TEMP_PROJECT)
                                        .tempDataset("")
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .build());
        assertTrue(ex.getMessage().contains("tempDataset is required"));
    }

    @Test
    public void buildIndirectNullTempProjectThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempDataset(TEMP_DATASET)
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .build());
        assertTrue(ex.getMessage().contains("tempProject is required"));
    }

    @Test
    public void buildIndirectEmptyTempProjectThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempProject("")
                                        .tempDataset(TEMP_DATASET)
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .build());
        assertTrue(ex.getMessage().contains("tempProject is required"));
    }

    @Test
    public void buildIndirectNullGcsTempPathThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .build());
        assertTrue(ex.getMessage().contains("tempGcsPath is required"));
    }

    @Test
    public void buildIndirectEmptyGcsTempPathThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath("")
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .build());
        assertTrue(ex.getMessage().contains("tempGcsPath is required"));
    }

    @Test
    public void buildIndirectNullJobProjectThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempProject(TEMP_PROJECT)
                                        .tempDataset(TEMP_DATASET)
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .build());
        assertTrue(ex.getMessage().contains("jobProject is required"));
    }

    @Test
    public void buildIndirectEmptyJobProjectThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempProject(TEMP_PROJECT)
                                        .tempDataset(TEMP_DATASET)
                                        .jobProject("")
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .build());
        assertTrue(ex.getMessage().contains("jobProject is required"));
    }

    @Test
    public void buildIndirectNullBulkWriterFactoryThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempProject(TEMP_PROJECT)
                                        .tempDataset(TEMP_DATASET)
                                        .jobProject(JOB_PROJECT)
                                        .build());
        assertTrue(ex.getMessage().contains("bulkWriterFactory is required"));
    }

    @Test
    public void buildIndirectNullFormatOptionsThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempProject(TEMP_PROJECT)
                                        .tempDataset(TEMP_DATASET)
                                        .jobProject(JOB_PROJECT)
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .build());
        assertTrue(ex.getMessage().contains("formatOptions is required"));
    }

    @Test
    public void buildIndirectWithCdcEnabledThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempProject(TEMP_PROJECT)
                                        .tempDataset(TEMP_DATASET)
                                        .jobProject(JOB_PROJECT)
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .formatOptions(FormatOptions.parquet())
                                        .enableCdc(true)
                                        .build());
        assertTrue(ex.getMessage().contains("CDC is not supported in INDIRECT"));
    }

    @Test
    public void buildIndirectWithTableCreationThrowsException() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                BigQuerySinkConfig.<String>newBuilder()
                                        .writeMode(WriteMode.INDIRECT)
                                        .connectOptions(indirectConnectOptions())
                                        .tempGcsPath(GCS_PATH)
                                        .tempProject(TEMP_PROJECT)
                                        .tempDataset(TEMP_DATASET)
                                        .jobProject(JOB_PROJECT)
                                        .bulkWriterFactory(dummyBulkWriterFactory())
                                        .formatOptions(FormatOptions.parquet())
                                        .enableTableCreation(true)
                                        .build());
        assertTrue(ex.getMessage().contains("Table auto-creation is not supported in INDIRECT"));
    }

    /** Minimal serializable BulkWriter.Factory - no-op, just exists to satisfy the builder. */
    private static final class DummyBulkWriterFactory
            implements BulkWriter.Factory<String>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public BulkWriter<String> create(FSDataOutputStream out) throws IOException {
            throw new UnsupportedOperationException("not used in test");
        }
    }
}
