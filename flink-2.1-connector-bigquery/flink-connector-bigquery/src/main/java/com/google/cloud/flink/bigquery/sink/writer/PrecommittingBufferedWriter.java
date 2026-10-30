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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;

import com.google.cloud.flink.bigquery.sink.TwoPhaseCommittingStatefulSink;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Adapts the Flink version independent {@link BigQueryBufferedWriter} to this Flink version's
 * two-phase committing and stateful sink writer interfaces.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
@Internal
public class PrecommittingBufferedWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, BigQueryWriterState, BigQueryCommittable> {

    private final BigQueryBufferedWriter<IN> delegate;

    public PrecommittingBufferedWriter(BigQueryBufferedWriter<IN> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(IN element, Context context) {
        delegate.write(element, context);
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        delegate.writeWatermark(watermark);
    }

    @Override
    public void flush(boolean endOfInput) {
        delegate.flush(endOfInput);
    }

    @Override
    public Collection<BigQueryCommittable> prepareCommit()
            throws IOException, InterruptedException {
        return delegate.prepareCommit();
    }

    @Override
    public List<BigQueryWriterState> snapshotState(long checkpointId) {
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @VisibleForTesting
    public BigQueryBufferedWriter<IN> getDelegate() {
        return delegate;
    }
}
