/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.sink.writer.selector;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.InstantiationUtil.isSerializable;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * build for {@link MessageMetadata}.
 *
 * @param <IN>
 */
public class MessageMetadataBuilder<IN> {

    private Function<IN, String> key;
    private Function<IN, byte[]> keyBytes;
    private Function<IN, byte[]> orderingKey;
    private Function<IN, Map<String, String>> properties;
    private Function<IN, Long> eventTime;
    private Function<IN, Long> sequenceId;

    public MessageMetadataBuilder<IN> setKey(SerializableFunction<IN, String> key) {
        this.key = checkNotNull(key);
        return this;
    }

    public MessageMetadataBuilder<IN> setKeyBytes(SerializableFunction<IN, byte[]> keyBytes) {
        this.keyBytes = checkNotNull(keyBytes);
        return this;
    }

    public MessageMetadataBuilder<IN> setOrderingKey(SerializableFunction<IN, byte[]> orderingKey) {
        this.orderingKey = checkNotNull(orderingKey);
        return this;
    }

    public MessageMetadataBuilder<IN> setProperties(
            SerializableFunction<IN, Map<String, String>> properties) {
        this.properties = checkNotNull(properties);
        return this;
    }

    public MessageMetadataBuilder<IN> setEventTime(SerializableFunction<IN, Long> eventTime) {
        this.eventTime = checkNotNull(eventTime);
        return this;
    }

    public MessageMetadataBuilder<IN> setSequenceId(SerializableFunction<IN, Long> sequenceId) {
        this.sequenceId = checkNotNull(sequenceId);
        return this;
    }

    public MessageMetadata<IN> build() {
        if (key != null && keyBytes != null) {
            throw new IllegalStateException("Only one of key and keyBytes can be selected");
        }

        // Since these implementations could be a lambda, make sure they are serializable.
        checkState(isSerializable(key), "key isn't serializable");
        checkState(isSerializable(keyBytes), "keyBytes isn't serializable");
        checkState(isSerializable(orderingKey), "orderingKey isn't serializable");
        checkState(isSerializable(properties), "properties isn't serializable");
        checkState(isSerializable(eventTime), "eventTime isn't serializable");
        checkState(isSerializable(sequenceId), "sequenceId isn't serializable");

        MessageMetadata<IN> metadata = new MessageMetadata<>();
        metadata.setKey(key);
        metadata.setKeyBytes(keyBytes);
        metadata.setOrderingKey(orderingKey);
        metadata.setProperties(properties);
        metadata.setEventTime(eventTime);
        metadata.setSequenceId(sequenceId);
        return metadata;
    }

    /**
     * Support Serializable Function.
     *
     * @param <T>
     * @param <R>
     */
    @FunctionalInterface
    public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
    }
}
