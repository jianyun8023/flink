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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.topic.TopicPartition;
import org.apache.flink.connector.pulsar.common.utils.TopicPartitionUtils;

import java.io.Serializable;

/**
 * Choose a pulsar topic for writing data.
 *
 * @param <IN> record data type.
 */
@PublicEvolving
@FunctionalInterface
public interface TopicSelector<IN> extends Serializable {

    /**
     * select topic name by record.
     *
     * @param record record
     * @return topicName
     */
    TopicPartition selector(IN record);

    default void open(Configuration configuration) {
        // TODO Add this method.
    }

    static <T> TopicSelector<T> singleTopic(String topic){
        final TopicPartition topicPartition = TopicPartitionUtils.fromTopicName(topic);
        return e -> topicPartition;
    }
    // TODO Create a round-robin topic selector which supports both single and multiple topics.
}
