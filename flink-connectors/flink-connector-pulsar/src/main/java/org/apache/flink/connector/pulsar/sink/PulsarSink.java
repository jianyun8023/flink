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

package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommitter;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriterFactory;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriterState;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriterStateSerializer;
import org.apache.flink.connector.pulsar.sink.writer.selector.TopicSelector;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchemaInitializationContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Sink implementation of Pulsar. Please use a {@link PulsarSinkBuilder} to construct a
 * {@link PulsarSink}. The following example shows how to create a PulsarSink receiving records
 * of <code>String</code> type.
 *
 * <pre>{@code
 * PulsarSink<String> sink = PulsarSink.builder()
 *      .setServiceUrl(operator().serviceUrl())
 *      .setAdminUrl(operator().adminUrl())
 *      .setTopic(topic)
 *      .setSerializationSchema(PulsarSerializationSchema.pulsarSchema(Schema.STRING))
 *      .build();
 * }</pre>
 *
 * <p>See {@link PulsarSinkBuilder} for more details.
 *
 * @param <IN> The input type of the source.
 */
@PublicEvolving
public class PulsarSink<IN> implements Sink<IN, PulsarSinkCommittable, PulsarWriterState, Void> {
    private static final long serialVersionUID = 4416714587951282119L;

    private final DeliveryGuarantee deliveryGuarantee;

    private final TopicSelector<IN> topicSelector;

    private final PulsarSerializationSchema<IN> serializationSchema;

    private final Configuration configuration;

    PulsarSink(
            DeliveryGuarantee deliveryGuarantee,
            TopicSelector<IN> topicSelector,
            PulsarSerializationSchema<IN> serializationSchema,
            Configuration configuration) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
        this.topicSelector = checkNotNull(topicSelector);
        this.serializationSchema = checkNotNull(serializationSchema);
        this.configuration = checkNotNull(configuration);
    }

    /**
     * Get a PulsarSinkBuilder to builder a {@link PulsarSink}.
     *
     * @return a Pulsar sink builder.
     */
    @SuppressWarnings("java:S4977")
    public static <IN> PulsarSinkBuilder<IN> builder() {
        return new PulsarSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN, PulsarSinkCommittable, PulsarWriterState> createWriter(
            InitContext initContext, List<PulsarWriterState> states) throws IOException {

        try {
            serializationSchema.open(new PulsarSerializationSchemaInitializationContext(initContext));
        } catch (Exception e) {
            PulsarExceptionUtils.sneakyThrow(e);
        }
        return PulsarWriterFactory.create(
                deliveryGuarantee,
                topicSelector,
                serializationSchema,
                configuration,
                initContext,
                states
        );
    }

    @Override
    public Optional<SimpleVersionedSerializer<PulsarWriterState>> getWriterStateSerializer() {
        return Optional.of(new PulsarWriterStateSerializer());
    }

    @Override
    public Optional<Committer<PulsarSinkCommittable>> createCommitter() throws IOException {
        return Optional.of(new PulsarCommitter(deliveryGuarantee, configuration));
    }

    @Override
    public Optional<GlobalCommitter<PulsarSinkCommittable, Void>> createGlobalCommitter()
            throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<PulsarSinkCommittable>> getCommittableSerializer() {
        return Optional.of(new PulsarSinkCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }
}
