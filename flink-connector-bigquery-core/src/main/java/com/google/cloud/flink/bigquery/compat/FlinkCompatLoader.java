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

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Loads the single {@link FlinkCompat} implementation bundled with the connector. */
@Internal
final class FlinkCompatLoader {

    private static volatile FlinkCompat instance;

    private FlinkCompatLoader() {}

    static FlinkCompat get() {
        FlinkCompat result = instance;
        if (result == null) {
            synchronized (FlinkCompatLoader.class) {
                result = instance;
                if (result == null) {
                    result = load();
                    instance = result;
                }
            }
        }
        return result;
    }

    private static FlinkCompat load() {
        List<FlinkCompat> implementations = new ArrayList<>();
        ServiceLoader.load(FlinkCompat.class, FlinkCompat.class.getClassLoader())
                .forEach(implementations::add);
        if (implementations.size() == 1) {
            return implementations.get(0);
        }
        if (implementations.isEmpty()) {
            throw new IllegalStateException(
                    "No FlinkCompat implementation found. The BigQuery connector core library"
                            + " must be used through one of the Flink version specific connector"
                            + " artifacts (e.g. flink-1.17-connector-bigquery).");
        }
        throw new IllegalStateException(
                "Found multiple FlinkCompat implementations, which means that BigQuery connector"
                        + " artifacts for different Flink versions are on the classpath: "
                        + implementations.stream()
                                .map(impl -> impl.getClass().getName())
                                .collect(Collectors.joining(", ")));
    }
}
