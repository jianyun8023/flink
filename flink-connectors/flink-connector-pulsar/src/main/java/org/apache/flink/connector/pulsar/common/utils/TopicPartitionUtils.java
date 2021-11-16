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

package org.apache.flink.connector.pulsar.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.common.topic.TopicPartition;
import org.apache.flink.connector.pulsar.common.topic.TopicRange;

import org.apache.pulsar.common.naming.TopicName;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Util for create Topic Partition. */
@Internal
public class TopicPartitionUtils {

    private TopicPartitionUtils() {
        // No public constructor
    }

    public static TopicPartition fromTopicName(TopicName topicName) {
        checkNotNull(topicName);
        if (topicName.isPartitioned()) {
            return new TopicPartition(topicName.getPartitionedTopicName(), topicName.getPartitionIndex(),
                    TopicRange.createFullRange());
        } else {
            return new TopicPartition(topicName.getPartitionedTopicName(), -1,
                    TopicRange.createFullRange());
        }
    }

    public static TopicPartition fromTopicName(String topicName) {
        checkNotNull(topicName);
        return fromTopicName(TopicName.get(topicName));
    }
}
