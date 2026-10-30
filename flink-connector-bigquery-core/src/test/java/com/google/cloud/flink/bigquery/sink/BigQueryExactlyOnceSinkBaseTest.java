/*
 * Copyright 2024 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryBufferedWriter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;

import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.fixedDelayRestartStrategyConfig;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQueryExactlyOnceSinkBase}. */
public class BigQueryExactlyOnceSinkBaseTest {

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
        assertNotNull(newSink(sinkConfig));
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
    public void testCreateWriter() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryExactlyOnceSinkBase<Object> exactlyOnceSink = newSink(sinkConfig);
        assertNotNull(
                exactlyOnceSink.createBufferedWriter(
                        1, 50, UnregisteredMetricsGroup.createSinkWriterMetricGroup()));
    }

    @Test
    public void testCreate_withMoreWritersThanAllowed() {
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
                                        .createBufferedWriter(
                                                1,
                                                129,
                                                UnregisteredMetricsGroup
                                                        .createSinkWriterMetricGroup()));
        assertThat(exception)
                .hasMessageThat()
                .contains("Attempting to create more Sink Writers than allowed");
    }

    @Test
    public void testRestoreWriter() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryExactlyOnceSinkBase<Object> exactlyOnceSink = newSink(sinkConfig);
        BigQueryBufferedWriter<Object> restoredWriter =
                exactlyOnceSink.restoreBufferedWriter(
                        1,
                        50,
                        UnregisteredMetricsGroup.createSinkWriterMetricGroup(),
                        Collections.singletonList(
                                new BigQueryWriterState(
                                        "some_stream", 100L, 210L, 200L, 100L, 3L)));
        BigQueryWriterState state = restoredWriter.snapshotState(4).get(0);
        assertEquals("some_stream", state.getStreamName());
        assertEquals(100, state.getStreamOffset());
        assertEquals(210, state.getTotalRecordsSeen());
        assertEquals(200, state.getTotalRecordsWritten());
        assertEquals(100, state.getTotalRecordsCommitted());
    }

    @Test
    public void testCreateCommitter() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryExactlyOnceSinkBase<Object> exactlyOnceSink = newSink(sinkConfig);
        assertNotNull(exactlyOnceSink.createBigQueryCommitter());
    }

    @Test
    public void testGetCommittableSerializer() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryExactlyOnceSinkBase<Object> exactlyOnceSink = newSink(sinkConfig);
        assertNotNull(exactlyOnceSink.getCommittableSerializer());
    }

    @Test
    public void testGetWriterStateSerializer() {
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryExactlyOnceSinkBase<Object> exactlyOnceSink = newSink(sinkConfig);
        assertNotNull(exactlyOnceSink.getWriterStateSerializer());
    }

    private static BigQueryConnectOptions getConnectOptions(boolean tableExists) {
        return StorageClientFaker.createConnectOptions(
                null,
                new StorageClientFaker.FakeBigQueryServices.FakeBigQueryStorageWriteClient(null),
                new StorageClientFaker.FakeBigQueryServices.FakeQueryDataClient(
                        tableExists, null, null, null));
    }

    private static <IN> BigQueryExactlyOnceSinkBase<IN> newSink(BigQuerySinkConfig<IN> sinkConfig) {
        return new BigQueryExactlyOnceSinkBase<IN>(sinkConfig) {};
    }
}
