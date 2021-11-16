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
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Pulsar Message Metadata support tool. This allows users to customize the metadata for each
 * Message in Pulsar Topic based on the current data.
 *
 * !!!Dropped!!!
 *
 * @param <IN> record
 */
public class MessageMetadata<IN> implements Serializable {

    private Function<IN, String> key;
    private Function<IN, byte[]> keyBytes;
    private Function<IN, byte[]> orderingKey;
    private Function<IN, Map<String, String>> properties;
    private Function<IN, Long> eventTime;
    private Function<IN, Long> sequenceId;

    public Function<IN, String> getKey() {
        return key;
    }

    public void setKey(Function<IN, String> key) {
        this.key = key;
    }

    public Function<IN, byte[]> getKeyBytes() {
        return keyBytes;
    }

    public void setKeyBytes(Function<IN, byte[]> keyBytes) {
        this.keyBytes = keyBytes;
    }

    public Function<IN, byte[]> getOrderingKey() {
        return orderingKey;
    }

    public void setOrderingKey(Function<IN, byte[]> orderingKey) {
        this.orderingKey = orderingKey;
    }

    public Function<IN, Map<String, String>> getProperties() {
        return properties;
    }

    public void setProperties(Function<IN, Map<String, String>> properties) {
        this.properties = properties;
    }

    public Function<IN, Long> getEventTime() {
        return eventTime;
    }

    public void setEventTime(Function<IN, Long> eventTime) {
        this.eventTime = eventTime;
    }

    public Function<IN, Long> getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(Function<IN, Long> sequenceId) {
        this.sequenceId = sequenceId;
    }

    public static <IN> MessageMetadataBuilder<IN> newBuilder() {
        return new MessageMetadataBuilder<>();
    }
}
