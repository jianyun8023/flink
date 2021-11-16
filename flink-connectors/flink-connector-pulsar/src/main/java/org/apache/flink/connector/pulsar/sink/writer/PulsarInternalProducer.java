package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils;
import org.apache.flink.connector.pulsar.sink.config.PulsarSinkConfigUtils;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Internal
public class PulsarInternalProducer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarInternalProducer.class);

    protected final Configuration configuration;

    private final Closer closer = Closer.create();

    private transient PulsarClientImpl pulsarClient;

    private final transient Map<String, Producer<byte[]>> topic2Producer;

    private volatile boolean inTransaction;

    public PulsarInternalProducer(Configuration configuration) {
        this.configuration = configuration;
        this.topic2Producer = new HashMap<>();
    }

    // ------------------------------internal method------------------------------

    public PulsarClientImpl getPulsarClient() {
        if (pulsarClient != null) {
            return pulsarClient;
        }
        pulsarClient = (PulsarClientImpl) PulsarConfigUtils.createClient(configuration);
        closer.register(pulsarClient);
        return pulsarClient;
    }

    public Producer<byte[]> getProducer(String topic) throws PulsarClientException {
        Producer<byte[]> producer = topic2Producer.get(topic);
        if (producer != null && producer.isConnected()) {
            return producer;
        }

        producer = PulsarSinkConfigUtils.createProducerBuilder(getPulsarClient(), Schema.BYTES, configuration)
                .topic(topic)
                .create();
        closer.register(producer);
        topic2Producer.put(topic, producer);
        return producer;
    }

    public void flushProducer() throws IOException {
        for (Producer<byte[]> p : topic2Producer.values()) {
            p.flush();
        }
    }

    @Override
    public void close() throws IOException {
        flushProducer();
        closer.close();
    }

    public void abortTransaction(List<PulsarWriterState> states) {
        final TransactionCoordinatorClientImpl tcClient = getPulsarClient().getTcClient();
        states.forEach(tcClient::abort);
    }

    public void beginTransaction() {
        super.beginTransaction();
        inTransaction = true;
    }

    public void abortTransaction() {
        LOG.debug("abortTransaction {}", transactionalId);
        checkState(inTransaction, "Transaction was not started");
        super.abortTransaction();
        inTransaction = false;
    }

    public void commitTransaction() {
        LOG.debug("commitTransaction {}", transactionalId);
        checkState(inTransaction, "Transaction was not started");
        super.commitTransaction();
        inTransaction = false;
    }

    public boolean isInTransaction() {
        return inTransaction;
    }
}
