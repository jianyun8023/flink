package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.writer.selector.TopicSelector;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;

import java.util.List;

@Internal
public class PulsarWriterFactory {

    public static <IN> BasePulsarWriter<IN> create(
            DeliveryGuarantee deliveryGuarantee,
            TopicSelector<IN> topicSelector,
            PulsarSerializationSchema<IN> serializationSchema,
            Configuration configuration,
            Sink.InitContext context,
            List<PulsarWriterState> states) {

//        AtLeastOncePulsarWriter
//        ExactlyOncePulsarWriter
        // TODO
        return null;
    }
}
