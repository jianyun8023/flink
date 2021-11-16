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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.apache.pulsar.client.api.transaction.TxnID;

import java.io.IOException;

/** a serializer for PulsarSinkCommittable. */
@Internal
public class PulsarSinkCommittableSerializer
        implements SimpleVersionedSerializer<PulsarSinkCommittable> {

    public static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PulsarSinkCommittable committable) throws IOException {
        // total size = boolean size + ( 2 * long size )
        DataOutputSerializer out = new DataOutputSerializer(129);
        if (committable.getTxnID() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(committable.getTxnID().getMostSigBits());
            out.writeLong(committable.getTxnID().getLeastSigBits());
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public PulsarSinkCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        if (version == VERSION) {
            return deserializeV1(in);
        }
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }

    private PulsarSinkCommittable deserializeV1(DataInputView dataInputView) throws IOException {
        TxnID transactionalId = null;
        if (dataInputView.readBoolean()) {
            long mostSigBits = dataInputView.readLong();
            long leastSigBits = dataInputView.readLong();
            transactionalId = new TxnID(mostSigBits, leastSigBits);
        }
        return new PulsarSinkCommittable(transactionalId);
    }
}
