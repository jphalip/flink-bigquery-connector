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

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;

import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.fixedDelayRestartStrategyConfig;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BigQueryDefaultSinkBase}. */
public class BigQueryDefaultSinkBaseTest {

    private StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        env =
                new StreamExecutionEnvironment(
                        fixedDelayRestartStrategyConfig(5, Duration.ofSeconds(3)));
    }

    @After
    public void tearDown() throws Exception {
        env.close();
    }

    @Test
    public void testConstructor() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryDefaultSinkBase<Object> sink = newSink(sinkConfig);
        assertEquals("projects/project/datasets/dataset/tables/table", sink.tablePath);
        assertNotNull(sink.serializer);
        assertNotNull(sink.schemaProvider.getAvroSchema());
        assertFalse(sink.schemaProvider.schemaUnknown());
        assertFalse(sink.enableTableCreation);
        assertFalse(sink.fatalizeSerializer);
    }

    @Test
    public void testConstructor_withoutConnectOptions() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(null)
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> newSink(sinkConfig));
        assertThat(exception)
                .hasMessageThat()
                .contains("connect options in sink config cannot be null");
    }

    @Test
    public void testConstructor_withoutSerializer() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(null)
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> newSink(sinkConfig));
        assertThat(exception).hasMessageThat().contains("serializer in sink config cannot be null");
    }

    @Test
    public void testConstructor_withCreateTable_withoutExistingTable() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(false))
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .enableTableCreation(true)
                        .build();
        BigQueryDefaultSinkBase<Object> sink = newSink(sinkConfig);
        assertNull(sink.schemaProvider.getAvroSchema());
        assertTrue(sink.schemaProvider.schemaUnknown());
        assertTrue(sink.enableTableCreation);
    }

    @Test
    public void testConstructor_withCreateTable_withExistingTable() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .enableTableCreation(true)
                        .build();
        BigQueryDefaultSinkBase<Object> sink = newSink(sinkConfig);
        assertNotNull(sink.schemaProvider.getAvroSchema());
        assertFalse(sink.schemaProvider.schemaUnknown());
        assertTrue(sink.enableTableCreation);
    }

    @Test
    public void testConstructor_withoutCreateTable_withoutExistingTable() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(false))
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalStateException exception =
                assertThrows(IllegalStateException.class, () -> newSink(sinkConfig));
        assertThat(exception)
                .hasMessageThat()
                .contains("table does not exist and table creation is not enabled");
    }

    @Test
    public void testCreateWriter() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryDefaultSinkBase<Object> defaultSink = newSink(sinkConfig);
        assertNotNull(
                defaultSink.createDefaultWriter(
                        1, 50, UnregisteredMetricsGroup.createSinkWriterMetricGroup()));
    }

    @Test
    public void testCreateWriter_withFatalSerializer() throws IOException, InterruptedException {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(FakeBigQuerySerializer.getErringSerializer())
                        .streamExecutionEnvironment(env)
                        .fatalizeSerializer(true)
                        .build();
        BigQueryDefaultSinkBase<Object> defaultSink = newSink(sinkConfig);
        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class,
                        () ->
                                defaultSink
                                        .createDefaultWriter(
                                                1,
                                                50,
                                                UnregisteredMetricsGroup
                                                        .createSinkWriterMetricGroup())
                                        .write(null, null));
        assertThat(exception).hasMessageThat().contains("Unable to serialize record");
    }

    @Test
    public void testCreateWriter_withMoreWritersThanAllowed() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                newSink(sinkConfig)
                                        .createDefaultWriter(
                                                1,
                                                129,
                                                UnregisteredMetricsGroup
                                                        .createSinkWriterMetricGroup()));
        assertThat(exception)
                .hasMessageThat()
                .contains("Attempting to create more Sink Writers than allowed");
    }

    @Test
    public void testRegionAndParallelism_datasetExists_providedRegionOverridden() {
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(
                                StorageClientFaker.createConnectOptionsForQuery(
                                        true, "us-central1"))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .region("us")
                        .build();
        BigQueryDefaultSinkBase<GenericRecord> sink = newSink(sinkConfig);
        assertEquals("us-central1", sink.region);
        assertEquals(128, sink.maxParallelism);
    }

    @Test
    public void testRegionAndParallelism_datasetExistsMultiRegion_providedRegionOverridden() {
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForQuery(true, "us"))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .region("us-central1")
                        .build();
        BigQueryDefaultSinkBase<GenericRecord> sink = newSink(sinkConfig);
        assertEquals("us", sink.region);
        assertEquals(512, sink.maxParallelism);
    }

    @Test
    public void testRegionAndParallelism_useProvidedRegion() {
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(
                                StorageClientFaker.createConnectOptionsForQuery(false, null))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .region("us-central1")
                        .build();
        BigQueryDefaultSinkBase<GenericRecord> sink = newSink(sinkConfig);
        assertEquals("us-central1", sink.region);
        assertEquals(128, sink.maxParallelism);
    }

    @Test
    public void testRegionAndParallelism_useProvidedMultiRegion() {
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(
                                StorageClientFaker.createConnectOptionsForQuery(false, null))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .region("US")
                        .build();
        BigQueryDefaultSinkBase<GenericRecord> sink = newSink(sinkConfig);
        assertEquals("US", sink.region);
        assertEquals(512, sink.maxParallelism);
    }

    @Test
    public void testRegionAndParallelism_useDefaultRegion() {
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(
                                StorageClientFaker.createConnectOptionsForQuery(false, null))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryDefaultSinkBase<GenericRecord> sink = newSink(sinkConfig);
        assertEquals("us", sink.region);
        assertEquals(512, sink.maxParallelism);
    }

    private static BigQueryConnectOptions getConnectOptions(boolean tableExists) {
        return StorageClientFaker.createConnectOptions(
                null,
                new StorageClientFaker.FakeBigQueryServices.FakeBigQueryStorageWriteClient(null),
                new StorageClientFaker.FakeBigQueryServices.FakeQueryDataClient(
                        tableExists, null, null, null));
    }

    private static <IN> BigQueryDefaultSinkBase<IN> newSink(BigQuerySinkConfig<IN> sinkConfig) {
        return new BigQueryDefaultSinkBase<IN>(sinkConfig) {};
    }
}
