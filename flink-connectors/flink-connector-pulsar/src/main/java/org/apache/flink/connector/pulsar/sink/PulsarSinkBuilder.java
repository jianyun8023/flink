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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.config.PulsarBaseBuilder;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.sink.writer.selector.TopicSelector;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_TRANSACTION_TIMEOUT_MILLIS;
import static org.apache.flink.connector.pulsar.sink.config.PulsarSinkConfigUtils.checkConfigurations;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class for {@link PulsarSink} to make it easier for the users to construct a {@link
 * PulsarSink}.
 *
 * <p>The following example shows the minimum setup to create a PulsarSink that reads the String
 * values from a Pulsar topic.
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
 * <p>The service url, admin url, topic to produce, and the record serializer
 * are required fields that must be set.
 *
 * <p>To specify the delivery guarantees of PulsarSink, one can call {@link #setDeliveryGuarantee(DeliveryGuarantee)}.
 * The default value of the delivery guarantee is {@link DeliveryGuarantee#EXACTLY_ONCE},
 * and it requires the Pulsar broker to turn on transaction support.
 *
 * <pre>{@code
 * PulsarSink<String> sink = PulsarSink.builder()
 *      .setServiceUrl(operator().serviceUrl())
 *      .setAdminUrl(operator().adminUrl())
 *      .setTopic(topic)
 *      .setSerializationSchema(PulsarSerializationSchema.pulsarSchema(Schema.STRING))
 *      .setDeliveryGuarantee(deliveryGuarantee)
 *      .build();
 * }</pre>
 *
 * @param <IN> The input type of the sink.
 */
@PublicEvolving
public class PulsarSinkBuilder<IN> extends PulsarBaseBuilder<PulsarSinkBuilder<IN>> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSinkBuilder.class);

    private DeliveryGuarantee deliveryGuarantee;
    private TopicSelector<IN> topicSelector;
    private PulsarSerializationSchema<IN> serializationSchema;

    // private builder constructor.
    PulsarSinkBuilder() {
        super();
        this.deliveryGuarantee = DeliveryGuarantee.EXACTLY_ONCE;
    }

    /**
     * Set the topic for the PulsarSink.
     *
     * @param topic pulsar topic
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> setTopic(String topic) {
        checkNotNull(topic);
        return setTopic(TopicSelector.singleTopic(topic));
    }

    /**
     * set a topic selector for the PulsarSink.
     *
     * @param topicSelector select a topic by record.
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> setTopic(TopicSelector<IN> topicSelector) {
        checkNotNull(topicSelector);
        checkArgument(this.topicSelector == null,
                "The topicSelector cannot be set repeatedly because it has already been set.");
        this.topicSelector = topicSelector;
        return this;
    }

    /**
     * Set a DeliverGuarantees for the PulsarSink.
     *
     * @param deliveryGuarantee deliver guarantees.
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    /**
     * SerializationSchema is required for getting the {@link TypeInformation} for message serialization in flink.
     *
     * <p>We have defined a set of implementations, using {@code
     * PulsarSerializationSchema#pulsarSchema} or {@code PulsarSerializationSchema#flinkSchema} for
     * creating the desired schema.
     */
    public <T extends IN> PulsarSinkBuilder<T> setSerializationSchema(
            PulsarSerializationSchema<T> serializationSchema) {
        PulsarSinkBuilder<T> self = specialized();
        self.serializationSchema = serializationSchema;
        return self;
    }

    /**
     * Build the {@link PulsarSink}.
     *
     * @return a PulsarSink with the settings made for this builder.
     */
    @SuppressWarnings("java:S3776")
    public PulsarSink<IN> build() {
        // Check builder configuration.
        checkConfigurations(configuration);

        // Ensure the topic  for pulsar.
        checkNotNull(topicSelector, "No topic names or topic pattern are provided.");

        if (DeliveryGuarantee.EXACTLY_ONCE == deliveryGuarantee) {
            configuration.set(PulsarOptions.PULSAR_ENABLE_TRANSACTION, true);
            // Need comments !!!
            // Due to the design of pulsar, the sendTimeoutMs=0 must be set to open the transaction.
            configuration.set(PulsarSinkOptions.PULSAR_SEND_TIMEOUT_MS, 0L);

            Long timeout = configuration.get(PULSAR_TRANSACTION_TIMEOUT_MILLIS);
            LOG.warn(
                    "The configured transaction timeout is {} milliseconds, "
                            + "make sure it was greater than your checkpoint interval.",
                    timeout);
        }

        // Make the configuration unmodifiable.
        UnmodifiableConfiguration config = new UnmodifiableConfiguration(configuration);
        return new PulsarSink<>(
                deliveryGuarantee, topicSelector, serializationSchema, config);
    }

    // ------------- private helpers  --------------

    /** Helper method for java compiler recognize the generic type. */
    @SuppressWarnings("unchecked")
    private <T extends IN> PulsarSinkBuilder<T> specialized() {
        return (PulsarSinkBuilder<T>) this;
    }
}
