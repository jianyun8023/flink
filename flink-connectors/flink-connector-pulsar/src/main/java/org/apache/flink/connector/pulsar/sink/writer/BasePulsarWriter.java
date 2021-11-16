package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.InitContextInitializationContextAdapter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.topic.TopicPartition;
import org.apache.flink.connector.pulsar.sink.PulsarSinkCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.selector.TopicSelector;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.metrics.MetricGroup;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public abstract class BasePulsarWriter<IN> implements SinkWriter<IN, PulsarSinkCommittable, PulsarWriterState> {
    protected final Configuration configuration;
    protected final SinkConfiguration sinkConfiguration;
    protected final TopicSelector<IN> topicSelector;
    protected final PulsarSerializationSchema<IN> serializationSchema;
    protected final PulsarInternalProducer pulsarInternalProducer;
    private final BiConsumer<MessageId, Throwable> sendCallback;

    public BasePulsarWriter(
            Configuration configuration,
            TopicSelector<IN> topicSelector,
            PulsarSerializationSchema<IN> serializationSchema,
            Sink.InitContext context) {
        this.configuration = configuration;
        this.sinkConfiguration = new SinkConfiguration(configuration);
        this.topicSelector = topicSelector;
        this.serializationSchema = serializationSchema;
        this.pulsarInternalProducer = new PulsarInternalProducer(configuration);
        this.sendCallback =
                initializeSendCallback(sinkConfiguration, context.getMailboxExecutor());

    }

    @Override
    public void write(IN value, Context context) throws IOException {
        TypedMessageBuilder<byte[]> messageBuilder;
        if (pulsarInternalProducer.isInTransaction()) {
            messageBuilder =
                    getCurrentProducer(value).newMessage(pulsarInternalProducer);
        } else {
            messageBuilder =
                    getCurrentProducer(value).newMessage();
        }

        serializationSchema.serialize(value, messageBuilder);
        CompletableFuture<MessageId> messageIdFuture = messageBuilder.sendAsync();
        messageIdFuture.whenComplete(sendCallback);
    }

    private Producer<byte[]> getCurrentProducer(IN value) throws PulsarClientException {
        final TopicPartition topicPartition = topicSelector.selector(value);
        return pulsarInternalProducer.getProducer(topicPartition.getFullTopicName());
    }

    @Override
    public List<PulsarSinkCommittable> prepareCommit(boolean flush) throws IOException {
        if (!flush) {
            return Collections.emptyList();
        }
        pulsarInternalProducer.flushProducer();
        return Collections.emptyList();
    }

    @Override
    public List<PulsarWriterState> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        pulsarInternalProducer.close();
    }

    protected abstract BiConsumer<MessageId, Throwable> initializeSendCallback(
            SinkConfiguration sinkConfiguration, MailboxExecutor mailboxExecutor);
}
