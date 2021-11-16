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

package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.InitContextInitializationContextAdapter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils;
import org.apache.flink.connector.pulsar.sink.PulsarSinkCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.selector.TopicSelector;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils.getId;

/**
 * a pulsar SinkWriter implement.
 *
 * @param <IN> record data type.
 */
@Internal
public class ExactlyOncePulsarWriter<IN> extends BasePulsarWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOncePulsarWriter.class);

    private Transaction currentTransaction;

    public ExactlyOncePulsarWriter(
            TopicSelector<IN> topicSelector,
            PulsarSerializationSchema<IN> serializationSchema,
            Configuration configuration,
            Sink.InitContext context) {
        super(configuration, topicSelector, serializationSchema, context);
    }

    //    @Override
    public void initializeState(List<PulsarWriterState> states) throws IOException {
        if (states.isEmpty()) {
            return;
        }
        pulsarInternalProducer.abortTransaction(states);
    }

    @Override
    public List<PulsarSinkCommittable> prepareCommit(boolean flush) throws IOException {
        if (!flush) {
            return Collections.emptyList();
        }
        if (currentTransaction == null) {
            LOG.debug("not init currentTransaction");
            return Collections.emptyList();
        }
        final TxnID txnID = getId(currentTransaction);
        return ImmutableList.of(new PulsarSinkCommittable(txnID));
    }

    @Override
    public List<PulsarWriterState> snapshotState(long checkpointId) throws IOException {
        LOG.debug("transaction is beginning in EXACTLY_ONCE mode");
        TxnID txnID = getId(currentTransaction);
        try {
            currentTransaction = createTransaction();
        } catch (Exception e) {
            PulsarExceptionUtils.sneakyThrow(e);
        }
        return Collections.singletonList(new PulsarWriterState(txnID));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    protected BiConsumer<MessageId, Throwable> initializeSendCallback(
            SinkConfiguration sinkConfiguration, MailboxExecutor mailboxExecutor) {

        final boolean failOnWrite = sinkConfiguration.isFailOnWrite();
        return (messageId, throwable) -> {
            if (failOnWrite && throwable != null) {
                mailboxExecutor.execute(
                        () -> {
                            throw new FlinkRuntimeException(throwable);
                        },
                        "Failed to send data to Pulsar");
            } else if (throwable != null) {
                LOG.warn(
                        "Error while sending message to Pulsar: {}, ignore the error because failOnWrite=false",
                        ExceptionUtils.stringifyException(throwable));
            }

        };
    }

    // ------------------------------internal method------------------------------

    /**
     * For each checkpoint we create new {@link Transaction} so that new transactions will not clash
     * with transactions created during previous checkpoints.
     */
    private Transaction createTransaction() throws Exception {
        return pulsarInternalProducer.getPulsarClient()
                .newTransaction()
                .withTransactionTimeout(
                        sinkConfiguration.getTransactionTimeoutMillis(), TimeUnit.MILLISECONDS)
                .build()
                .get();
    }

    private Transaction getCurrentTransaction() {
        try {
            if (currentTransaction == null) {
                currentTransaction = createTransaction();
            }

        } catch (Exception e) {
            PulsarExceptionUtils.sneakyThrow(e);
        }
        return currentTransaction;
    }
}
