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

package com.google.cloud.flink.bigquery.sink.writer;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittable;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PrecommittingBufferedWriter}. */
public class PrecommittingBufferedWriterTest {

    @Test
    public void testDelegatesToBufferedWriter() throws Exception {
        @SuppressWarnings("unchecked")
        BigQueryBufferedWriter<Object> delegate = mock(BigQueryBufferedWriter.class);
        Collection<BigQueryCommittable> committables =
                Collections.singletonList(new BigQueryCommittable(1, "stream", 10L));
        List<BigQueryWriterState> state =
                Collections.singletonList(new BigQueryWriterState("stream", 10L, 20L, 20L, 0L, 3L));
        when(delegate.prepareCommit()).thenReturn(committables);
        when(delegate.snapshotState(3L)).thenReturn(state);
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        Watermark watermark = new Watermark(42L);

        PrecommittingBufferedWriter<Object> writer = new PrecommittingBufferedWriter<>(delegate);
        Object element = new Object();
        writer.write(element, context);
        writer.writeWatermark(watermark);
        writer.flush(true);
        assertThat(writer.prepareCommit()).isSameInstanceAs(committables);
        assertThat(writer.snapshotState(3L)).isSameInstanceAs(state);
        writer.close();

        verify(delegate).write(element, context);
        verify(delegate).writeWatermark(watermark);
        verify(delegate).flush(true);
        verify(delegate).close();
        assertThat(writer.getDelegate()).isSameInstanceAs(delegate);
        Mockito.verifyNoMoreInteractions(context);
    }
}
