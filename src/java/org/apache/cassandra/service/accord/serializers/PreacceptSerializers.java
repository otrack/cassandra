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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import javax.annotation.Nullable;

import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.PreAccept.PreAcceptReply;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.serializers.TxnRequestSerializer.WithUnsyncedSerializer;


public class PreacceptSerializers
{
    private PreacceptSerializers() {}

    public static final IVersionedSerializer<PreAccept> request = new WithUnsyncedSerializer<>()
    {
        @Override
        public void serializeBody(PreAccept msg, DataOutputPlus out, int version) throws IOException
        {
            int flags = (msg.partialDeps == null ? 0 : 1)
                        | (msg.route == null ? 0 : 2)
                        | (msg.hasCoordinatorVote ? 4 : 0)
                        | (msg.acceptEpoch == msg.minEpoch ? 0 : 8);
            out.writeByte(flags);
            CommandSerializers.partialTxn.serialize(msg.partialTxn, out, version);
            if (msg.partialDeps != null)
                DepsSerializers.partialDeps.serialize(msg.partialDeps, out, version);
            if (msg.route != null)
                KeySerializers.fullRoute.serialize(msg.route, out, version);
            if (msg.acceptEpoch != msg.minEpoch)
                out.writeUnsignedVInt(msg.acceptEpoch - msg.minEpoch);
        }

        @Override
        public PreAccept deserializeBody(DataInputPlus in, int version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            byte flags = in.readByte();
            PartialTxn partialTxn = CommandSerializers.partialTxn.deserialize(in, version);
            @Nullable PartialDeps partialDeps = (flags & 1) == 0 ? null : DepsSerializers.partialDeps.deserialize(in, version);
            @Nullable FullRoute<?> fullRoute = (flags & 2) == 0 ? null : KeySerializers.fullRoute.deserialize(in, version);
            boolean hasCoordinatorVote = (flags & 4) != 0;
            long acceptEpoch = (flags & 8) == 0 ? minEpoch : in.readUnsignedVInt() + minEpoch;
            return PreAccept.SerializerSupport.create(txnId, scope, waitForEpoch, minEpoch, acceptEpoch, partialTxn, partialDeps, hasCoordinatorVote, fullRoute);
        }

        @Override
        public long serializedBodySize(PreAccept msg, int version)
        {
            return   TypeSizes.BYTE_SIZE
                   + CommandSerializers.partialTxn.serializedSize(msg.partialTxn, version)
                   + (msg.partialDeps == null ? 0 : DepsSerializers.partialDeps.serializedSize(msg.partialDeps, version))
                   + (msg.route == null ? 0 : KeySerializers.fullRoute.serializedSize(msg.route, version))
                   + (msg.acceptEpoch == msg.minEpoch ? 0 : TypeSizes.sizeofUnsignedVInt(msg.acceptEpoch - msg.minEpoch));
        }
    };

    public static final IVersionedSerializer<PreAcceptReply> reply = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(PreAcceptReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOk());
            if (!reply.isOk())
                return;

            PreAcceptOk preAcceptOk = (PreAcceptOk) reply;
            CommandSerializers.txnId.serialize(preAcceptOk.txnId, out, version);
            ExecuteAtSerializer.serialize(preAcceptOk.txnId, preAcceptOk.witnessedAt, out);
            DepsSerializers.deps.serialize(preAcceptOk.deps, out, version);
        }

        @Override
        public PreAcceptReply deserialize(DataInputPlus in, int version) throws IOException
        {
            if (!in.readBoolean())
                return PreAccept.PreAcceptNack.INSTANCE;

            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            return new PreAcceptOk(txnId,
                                   ExecuteAtSerializer.deserialize(txnId, in),
                                   DepsSerializers.deps.deserialize(in, version));
        }

        @Override
        public long serializedSize(PreAcceptReply reply, int version)
        {
            long size = TypeSizes.sizeof(reply.isOk());
            if (!reply.isOk())
                return size;

            PreAcceptOk preAcceptOk = (PreAcceptOk) reply;
            size += CommandSerializers.txnId.serializedSize(preAcceptOk.txnId, version);
            size += ExecuteAtSerializer.serializedSize(preAcceptOk.txnId, preAcceptOk.witnessedAt);
            size += DepsSerializers.deps.serializedSize(preAcceptOk.deps, version);

            return size;
        }
    };
}
