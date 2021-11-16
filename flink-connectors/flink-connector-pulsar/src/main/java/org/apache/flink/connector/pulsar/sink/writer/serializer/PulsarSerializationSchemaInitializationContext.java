/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.sink.writer.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

/**
 * Convert the {@link InitContext} into a {@link
 * SerializationSchema.InitializationContext}, we would use a pulsar named metric group for this
 * content.
 */
@Internal
public class PulsarSerializationSchemaInitializationContext
        implements SerializationSchema.InitializationContext {

    private final InitContext context;

    public PulsarSerializationSchemaInitializationContext(InitContext context) {
        this.context = context;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return context.metricGroup().addGroup("pulsarSerializer");
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return context.getUserCodeClassLoader();
    }
}
