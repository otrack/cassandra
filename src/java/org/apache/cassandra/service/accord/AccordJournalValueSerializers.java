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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

import com.google.common.collect.ImmutableSortedMap;

import accord.api.Journal;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;

import static accord.api.Journal.Load.ALL;
import static accord.local.CommandStores.RangesForEpoch;

// TODO (required): test with large collection values, and perhaps split out some fields if they have a tendency to grow larger
// TODO (required): alert on metadata size
// TODO (required): versioning
public class AccordJournalValueSerializers
{
    private static final int messagingVersion = MessagingService.VERSION_40;
    public interface FlyweightSerializer<ENTRY, IMAGE>
    {
        IMAGE mergerFor(JournalKey key);

        void serialize(JournalKey key, ENTRY from, DataOutputPlus out, int userVersion) throws IOException;

        void reserialize(JournalKey key, IMAGE from, DataOutputPlus out, int userVersion) throws IOException;

        void deserialize(JournalKey key, IMAGE into, DataInputPlus in, int userVersion) throws IOException;
    }

    public static class CommandDiffSerializer
    implements FlyweightSerializer<AccordJournal.Writer, AccordJournal.Builder>
    {
        @Override
        public AccordJournal.Builder mergerFor(JournalKey journalKey)
        {
            return new AccordJournal.Builder(journalKey.id, ALL);
        }

        @Override
        public void serialize(JournalKey key, AccordJournal.Writer writer, DataOutputPlus out, int userVersion)
        {
            try
            {
                writer.write(out, userVersion);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reserialize(JournalKey key, AccordJournal.Builder from, DataOutputPlus out, int userVersion) throws IOException
        {
            from.serialize(out,
                           // In CompactionIterator, we are dealing with relatively recent records, so we do not pass redundant before here.
                           // However, we do on load and during Journal SSTable compaction.
                           RedundantBefore.EMPTY,
                           userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, AccordJournal.Builder into, DataInputPlus in, int userVersion) throws IOException
        {
            into.deserializeNext(in, userVersion);
        }
    }

    public abstract static class Accumulator<A, V>
    {
        protected A accumulated;

        public Accumulator(A initial)
        {
            this.accumulated = initial;
        }

        protected void update(V newValue)
        {
            accumulated = accumulate(accumulated, newValue);
        }

        protected abstract A accumulate(A oldValue, V newValue);

        public A get()
        {
            return accumulated;
        }
    }

    public static class IdentityAccumulator<T> extends Accumulator<T, T>
    {
        boolean hasRead;
        public IdentityAccumulator(T initial)
        {
            super(initial);
        }

        @Override
        protected T accumulate(T oldValue, T newValue)
        {
            if (hasRead)
                return oldValue;
            hasRead = true;
            return newValue;
        }
    }

    public static class RedundantBeforeSerializer
    implements FlyweightSerializer<RedundantBefore, IdentityAccumulator<RedundantBefore>>
    {
        @Override
        public IdentityAccumulator<RedundantBefore> mergerFor(JournalKey journalKey)
        {
            return new IdentityAccumulator<>(RedundantBefore.EMPTY);
        }

        @Override
        public void serialize(JournalKey key, RedundantBefore entry, DataOutputPlus out, int userVersion)
        {
            try
            {
                if (entry == RedundantBefore.EMPTY)
                {
                    out.writeInt(0);
                    return;
                }
                out.writeInt(1);
                CommandStoreSerializers.redundantBefore.serialize(entry, out, messagingVersion);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reserialize(JournalKey key, IdentityAccumulator<RedundantBefore> from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, IdentityAccumulator<RedundantBefore> into, DataInputPlus in, int userVersion) throws IOException
        {
            if (in.readInt() == 0)
            {
                into.update(RedundantBefore.EMPTY);
                return;
            }
            into.update(CommandStoreSerializers.redundantBefore.deserialize(in, messagingVersion));
        }
    }

    public static class DurableBeforeAccumulator extends Accumulator<DurableBefore, DurableBefore>
    {
        public DurableBeforeAccumulator()
        {
            super(DurableBefore.EMPTY);
        }

        @Override
        protected DurableBefore accumulate(DurableBefore oldValue, DurableBefore newValue)
        {
            return DurableBefore.merge(oldValue, newValue);
        }
    }

    public static class DurableBeforeSerializer
    implements FlyweightSerializer<DurableBefore, DurableBeforeAccumulator>
    {
        public DurableBeforeAccumulator mergerFor(JournalKey journalKey)
        {
            return new DurableBeforeAccumulator();
        }

        @Override
        public void serialize(JournalKey key, DurableBefore entry, DataOutputPlus out, int userVersion)
        {
            try
            {
                CommandStoreSerializers.durableBefore.serialize(entry, out, messagingVersion);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reserialize(JournalKey key, DurableBeforeAccumulator from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, DurableBeforeAccumulator into, DataInputPlus in, int userVersion) throws IOException
        {
            // TODO: maybe using local serializer is not the best call here, but how do we distinguish
            // between messaging and disk versioning?
            into.update(CommandStoreSerializers.durableBefore.deserialize(in, messagingVersion));
        }
    }

    public static class BootstrapBeganAtSerializer
    implements FlyweightSerializer<NavigableMap<TxnId, Ranges>, IdentityAccumulator<NavigableMap<TxnId, Ranges>>>
    {
        @Override
        public IdentityAccumulator<NavigableMap<TxnId, Ranges>> mergerFor(JournalKey key)
        {
            return new IdentityAccumulator<>(ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<TxnId, Ranges> entry, DataOutputPlus out, int userVersion) throws IOException
        {
            CommandStoreSerializers.bootstrapBeganAt.serialize(entry, out, messagingVersion);
        }

        @Override
        public void reserialize(JournalKey key, IdentityAccumulator<NavigableMap<TxnId, Ranges>> image, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, image.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, IdentityAccumulator<NavigableMap<TxnId, Ranges>> into, DataInputPlus in, int userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.bootstrapBeganAt.deserialize(in, messagingVersion));
        }
    }

    public static class SafeToReadSerializer
    implements FlyweightSerializer<NavigableMap<Timestamp, Ranges>, IdentityAccumulator<NavigableMap<Timestamp, Ranges>>>
    {
        @Override
        public IdentityAccumulator<NavigableMap<Timestamp, Ranges>> mergerFor(JournalKey key)
        {
            return new IdentityAccumulator<>(ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<Timestamp, Ranges> from, DataOutputPlus out, int userVersion) throws IOException
        {
            CommandStoreSerializers.safeToRead.serialize(from, out, messagingVersion);
        }

        @Override
        public void reserialize(JournalKey key, IdentityAccumulator<NavigableMap<Timestamp, Ranges>> from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, IdentityAccumulator<NavigableMap<Timestamp, Ranges>> into, DataInputPlus in, int userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.safeToRead.deserialize(in, messagingVersion));
        }
    }

    public static class RangesForEpochSerializer
    implements FlyweightSerializer<RangesForEpoch, Accumulator<RangesForEpoch, RangesForEpoch>>
    {
        public IdentityAccumulator<RangesForEpoch> mergerFor(JournalKey key)
        {
            return new IdentityAccumulator<>(null);
        }

        @Override
        public void serialize(JournalKey key, RangesForEpoch from, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeUnsignedVInt32(from.size());
            from.forEach((epoch, ranges) -> {
                try
                {
                    out.writeLong(epoch);
                    KeySerializers.ranges.serialize(ranges, out, messagingVersion);
                }
                catch (Throwable t)
                {
                    throw new IllegalStateException("Serialization error", t);
                }
            });
        }

        @Override
        public void reserialize(JournalKey key, Accumulator<RangesForEpoch, RangesForEpoch> from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, messagingVersion);
        }

        @Override
        public void deserialize(JournalKey key, Accumulator<RangesForEpoch, RangesForEpoch> into, DataInputPlus in, int userVersion) throws IOException
        {
            int size = in.readUnsignedVInt32();
            Ranges[] ranges = new Ranges[size];
            long[] epochs = new long[size];
            for (int i = 0; i < ranges.length; i++)
            {
                epochs[i] = in.readLong();
                ranges[i] = KeySerializers.ranges.deserialize(in, messagingVersion);
            }
            Invariants.require(ranges.length == epochs.length);
            into.update(new RangesForEpoch(epochs, ranges));
        }
    }

    public static class MapAccumulator<K extends Comparable<K>, V> extends Accumulator<NavigableMap<K, V>, V>
    {
        private final Function<V, K> getKey;

        public MapAccumulator(Function<V, K> getKey)
        {
            super(new TreeMap<>());
            this.getKey = getKey;
        }

        @Override
        protected NavigableMap<K, V> accumulate(NavigableMap<K, V> accumulator, V newValue)
        {
            V prev = accumulator.put(getKey.apply(newValue), newValue);
            Invariants.require(prev == null || prev.equals(newValue));
            return accumulator;
        }
    }

    public static class TopologyUpdateSerializer
    implements FlyweightSerializer<Journal.TopologyUpdate, MapAccumulator<Long, Journal.TopologyUpdate>>
    {
        private final RangesForEpochSerializer rangesForEpochSerializer = new RangesForEpochSerializer();

        @Override
        public MapAccumulator<Long, Journal.TopologyUpdate> mergerFor(JournalKey key)
        {
            return new MapAccumulator<>(topologyUpdate -> topologyUpdate.global.epoch());
        }

        @Override
        public void serialize(JournalKey key, Journal.TopologyUpdate from, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeInt(1);
            serializeOne(key, from, out, userVersion);
        }

        private void serializeOne(JournalKey key, Journal.TopologyUpdate from, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeInt(from.commandStores.size());
            for (Map.Entry<Integer, RangesForEpoch> e : from.commandStores.entrySet())
            {
                out.writeInt(e.getKey());
                rangesForEpochSerializer.serialize(key, e.getValue(), out, userVersion);
            }
            TopologySerializers.topology.serialize(from.local, out, userVersion);
            TopologySerializers.topology.serialize(from.global, out, userVersion);
        }

        @Override
        public void reserialize(JournalKey key, MapAccumulator<Long, Journal.TopologyUpdate> from, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeInt(from.accumulated.size());
            for (Journal.TopologyUpdate update : from.accumulated.values())
                serializeOne(key, update, out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, MapAccumulator<Long, Journal.TopologyUpdate> into, DataInputPlus in, int userVersion) throws IOException
        {
            int size = in.readInt();
            Accumulator<RangesForEpoch, RangesForEpoch> acc = new Accumulator<>(null)
            {
                @Override
                protected RangesForEpoch accumulate(RangesForEpoch oldValue, RangesForEpoch newValue)
                {
                    return this.accumulated = newValue;
                }
            };
            for (int i = 0; i < size; i++)
            {
                int commandStoresSize = in.readInt();
                Int2ObjectHashMap<RangesForEpoch> commandStores = new Int2ObjectHashMap<>();
                for (int j = 0; j < commandStoresSize; j++)
                {
                    acc.update(null);
                    int commandStoreId = in.readInt();
                    rangesForEpochSerializer.deserialize(key, acc, in, userVersion);
                    commandStores.put(commandStoreId, acc.accumulated);
                }
                Topology local = TopologySerializers.topology.deserialize(in, userVersion);
                Topology global = TopologySerializers.topology.deserialize(in, userVersion);
                into.update(new Journal.TopologyUpdate(commandStores, local, global));
            }
        }
    }
}