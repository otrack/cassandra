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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.CommonAttributes.Mutable;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.CommandsForKey.InternalStatus;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.local.cfk.Serialize;
import accord.primitives.Ballot;
import accord.primitives.KeyDeps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.RangeDeps;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.SortedArrays;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.simulator.RandomSource.Choices;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;

import static accord.local.cfk.CommandsForKey.NO_BOUNDS_INFO;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtErased;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtUnknown;
import static accord.primitives.Status.Durability.Majority;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.utils.Property.qt;
import static accord.utils.SortedArrays.Search.FAST;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;

// TODO (required): test statusOverrides
public class CommandsForKeySerializerTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        // need to create the accord test table as generating random txn is not currently supported
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        StorageService.instance.initServer();
    }

    @Before
    public void before() throws Throwable
    {
        CommandsForKey.disableLinearizabilityViolationsReporting();
    }

    @After
    public void after() throws Throwable
    {
        CommandsForKey.enableLinearizabilityViolationsReporting();
    }

    static class Cmd
    {
        final TxnId txnId;
        final SaveStatus saveStatus;
        final PartialTxn txn;
        final Timestamp executeAt;
        final Ballot ballot;
        final boolean isDurable;
        final List<TxnId> deps = new ArrayList<>();
        final List<TxnId> missing = new ArrayList<>();
        boolean invisible;

        Cmd(TxnId txnId, PartialTxn txn, SaveStatus saveStatus, boolean isDurable, Timestamp executeAt, Ballot ballot)
        {
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.txn = txn;
            this.executeAt = executeAt;
            this.ballot = ballot;
            this.isDurable = isDurable;
        }

        CommonAttributes attributes()
        {
            Mutable mutable = new Mutable(txnId);
            if (saveStatus.known.isDefinitionKnown())
                mutable.partialTxn(txn);

            mutable.setParticipants(StoreParticipants.all(txn.keys().toRoute(txn.keys().get(0).someIntersectingRoutingKey(null))));
            mutable.durability(isDurable ? Majority : NotDurable);
            if (saveStatus.known.deps.hasProposedOrDecidedDeps())
            {
                try (KeyDeps.Builder builder = KeyDeps.builder();)
                {
                    for (TxnId id : deps)
                        builder.add(((Key)txn.keys().get(0)).toUnseekable(), id);
                    mutable.partialDeps(new PartialDeps(AccordTestUtils.fullRange(txn), builder.build(), RangeDeps.NONE, KeyDeps.NONE));
                }
            }

            return mutable;
        }

        Command toCommand()
        {
            switch (saveStatus)
            {
                default: throw new AssertionError("Unhandled saveStatus: " + saveStatus);
                case Uninitialised:
                case NotDefined:
                    return Command.SerializerSupport.notDefined(attributes(), Ballot.ZERO);
                case PreAccepted:
                    return Command.SerializerSupport.preaccepted(attributes(), executeAt, Ballot.ZERO);
                case AcceptedInvalidate:
                    return Command.SerializerSupport.acceptedInvalidateWithoutDefinition(attributes(), ballot, ballot);
                case Accepted:
                case AcceptedWithDefinition:
                case AcceptedInvalidateWithDefinition:
                case PreCommittedWithDefinition:
                case PreCommittedWithDefinitionAndAcceptedDeps:
                case PreCommittedWithAcceptedDeps:
                case PreCommitted:
                    return Command.SerializerSupport.accepted(attributes(), saveStatus, executeAt, ballot, ballot);

                case Committed:
                    return Command.SerializerSupport.committed(attributes(), saveStatus, executeAt, ballot, ballot, null);

                case Stable:
                case ReadyToExecute:
                    return Command.SerializerSupport.committed(attributes(), saveStatus, executeAt, ballot, ballot, Command.WaitingOn.empty(txnId.domain()));

                case PreApplied:
                case Applying:
                case Applied:
                    return Command.SerializerSupport.executed(attributes(), saveStatus, executeAt, ballot, ballot, Command.WaitingOn.empty(txnId.domain()), new Writes(txnId, executeAt, txn.keys(), new TxnWrite(Collections.emptyList(), true)), new TxnData());

                case Invalidated:
                    return Command.SerializerSupport.invalidated(txnId);
            }
        }

        @Override
        public String toString()
        {
            return "Cmd{" +
                   "txnId=" + txnId +
                   ", saveStatus=" + saveStatus +
                   ", txn=" + txn +
                   ", executeAt=" + executeAt +
                   ", deps=" + deps +
                   ", missing=" + missing +
                   ", invisible=" + invisible +
                   '}';
        }
    }

    static class ObjectGraph
    {
        final Cmd[] cmds;
        ObjectGraph(Cmd[] cmds)
        {
            this.cmds = cmds;
        }

        List<Command> toCommands()
        {
            List<Command> commands = new ArrayList<>(cmds.length);
            for (int i = 0 ; i < cmds.length ; ++i)
                commands.add(cmds[i].toCommand());
            return commands;
        }
    }

    private static ObjectGraph generateObjectGraph(int txnIdCount, Supplier<TxnId> txnIdSupplier, Supplier<SaveStatus> saveStatusSupplier, Function<TxnId, PartialTxn> txnSupplier, Function<TxnId, Timestamp> timestampSupplier, Supplier<Ballot> ballotSupplier, IntSupplier missingCountSupplier, RandomSource source)
    {
        Cmd[] cmds = new Cmd[txnIdCount];
        for (int i = 0 ; i < txnIdCount ; ++i)
        {
            TxnId txnId = txnIdSupplier.get();
            SaveStatus saveStatus = saveStatusSupplier.get();
            Timestamp executeAt = txnId;
            if (!txnId.kind().awaitsOnlyDeps() && saveStatus.known.executeAt != ExecuteAtErased && saveStatus.known.executeAt != ExecuteAtUnknown)
                executeAt = timestampSupplier.apply(txnId);

            boolean isDurable = false;
            Ballot ballot;
            switch (saveStatus.status)
            {
                default: throw new AssertionError();
                case NotDefined:
                case PreAccepted:
                case Invalidated:
                case Truncated:
                    ballot = Ballot.ZERO;
                    break;
                case PreApplied:
                case Applied:
                    isDurable = source.nextBoolean();
                case AcceptedInvalidate:
                case Accepted:
                case PreCommitted:
                case Committed:
                case Stable:
                    ballot = ballotSupplier.get();
            }

            cmds[i] = new Cmd(txnId, txnSupplier.apply(txnId), saveStatus, isDurable, executeAt, ballot);
        }
        Arrays.sort(cmds, Comparator.comparing(o -> o.txnId));
        for (int i = 0 ; i < txnIdCount ; ++i)
        {
            if (!cmds[i].saveStatus.known.deps.hasProposedOrDecidedDeps())
                continue;

            Timestamp knownBefore = cmds[i].saveStatus.known.deps.hasCommittedOrDecidedDeps() ? cmds[i].executeAt : cmds[i].txnId;
            int limit = SortedArrays.binarySearch(cmds, 0, cmds.length, knownBefore, (a, b) -> a.compareTo(b.txnId), FAST);
            if (limit < 0) limit = -1 - limit;

            List<TxnId> deps = cmds[i].deps;
            List<TxnId> missing = cmds[i].missing;
            for (int j = 0 ; j < limit ; ++j)
            {
                if (i != j && cmds[i].txnId.kind().witnesses(cmds[j].txnId))
                    deps.add(cmds[j].txnId);
            }

            int missingCount = Math.min(deps.size(), missingCountSupplier.getAsInt());
            while (missingCount > 0)
            {
                int remove = source.nextInt(deps.size());
                int cmdIndex = SortedArrays.binarySearch(cmds, 0, cmds.length, deps.get(remove), (a, b) -> a.compareTo(b.txnId), FAST);
                if (!cmds[cmdIndex].saveStatus.hasBeen(Status.Committed))
                    missing.add(deps.get(remove));
                deps.set(remove, deps.get(deps.size() - 1));
                deps.remove(deps.size() - 1);
                --missingCount;
            }
            deps.sort(TxnId::compareTo);
            missing.sort(TxnId::compareTo);
        }

        outer: for (int i = 0 ; i < cmds.length ; ++i)
        {
            if (null != InternalStatus.from(cmds[i].saveStatus))
                continue;

            for (int j = 0 ; j < i ; ++j)
            {
                InternalStatus status = InternalStatus.from(cmds[j].saveStatus);
                if (status == null || !status.hasExecuteAtAndDeps) continue;
                if (cmds[j].txnId.kind().witnesses(cmds[i].txnId) && status.depsKnownBefore(cmds[j].txnId, cmds[j].executeAt).compareTo(cmds[i].txnId) > 0 && Collections.binarySearch(cmds[j].missing, cmds[i].txnId) < 0)
                    continue outer;
            }
            for (int j = i + 1 ; j < cmds.length ; ++j)
            {
                InternalStatus status = InternalStatus.from(cmds[j].saveStatus);
                if (status == null || !status.hasExecuteAtAndDeps) continue;
                if (cmds[j].txnId.kind().witnesses(cmds[i].txnId) && Collections.binarySearch(cmds[j].missing, cmds[i].txnId) < 0)
                    continue outer;
            }
            cmds[i].invisible = true;
            for (int j = 0 ; j < i ; ++j)
            {
                if (cmds[j].executeAt.compareTo(cmds[i].txnId) > 0)
                {
                    int remove = Collections.binarySearch(cmds[j].missing, cmds[i].txnId);
                    if (remove >= 0) cmds[j].missing.remove(remove);
                }
            }
            for (int j = i + 1 ; j < cmds.length ; ++j)
            {
                int remove = Collections.binarySearch(cmds[j].missing, cmds[i].txnId);
                if (remove >= 0) cmds[j].missing.remove(remove);
            }
        }
        return new ObjectGraph(cmds);
    }

    private static Function<Timestamp, TxnId> txnIdSupplier(LongUnaryOperator epochSupplier, LongUnaryOperator hlcSupplier, Supplier<Txn.Kind> kindSupplier, Supplier<Node.Id> idSupplier)
    {
        return min -> new TxnId(epochSupplier.applyAsLong(min == null ? 1 : min.epoch()), hlcSupplier.applyAsLong(min == null ? 1 : min.hlc() + 1), kindSupplier.get(), Routable.Domain.Key, idSupplier.get());
    }

    private static Function<Timestamp, Timestamp> timestampSupplier(LongUnaryOperator epochSupplier, LongUnaryOperator hlcSupplier, IntSupplier flagSupplier, Supplier<Node.Id> idSupplier)
    {
        return min -> Timestamp.fromValues(epochSupplier.applyAsLong(min == null ? 1 : min.epoch()), hlcSupplier.applyAsLong(min == null ? 1 : min.hlc() + 1), flagSupplier.getAsInt(), idSupplier.get());
    }

    private static Supplier<Ballot> ballotSupplier(LongUnaryOperator epochSupplier, LongUnaryOperator hlcSupplier, IntSupplier flagSupplier, Supplier<Node.Id> idSupplier)
    {
        return () -> Ballot.fromValues(epochSupplier.applyAsLong(1), hlcSupplier.applyAsLong(1), flagSupplier.getAsInt(), idSupplier.get());
    }

    private static <T extends Timestamp> Function<Timestamp, T> timestampSupplier(Set<Timestamp> unique, Function<Timestamp, T> supplier)
    {
        return min -> {
            T candidate = supplier.apply(min);
            while (!unique.add(candidate))
            {
                T next = supplier.apply(min);
                if (next.equals(candidate)) min = candidate;
                else candidate = next;
            }
            return candidate;
        };
    }

    @Test
    public void serde()
    {
        testOne(3466420662549679178L);
        Random random = new Random();
        for (int i = 0 ; i < 10000 ; ++i)
        {
            long seed = random.nextLong();
            testOne(seed);
        }
    }

    private static void testOne(long seed)
    {
        try
        {
            System.out.println(seed);
            RandomSource source = RandomSource.wrap(new Random(seed));

            // TODO (required): produce broader variety of distributions, including executeAt with lower HLC but higher epoch
            final LongUnaryOperator epochSupplier; {
                long maxEpoch = source.nextLong(1, 10);
                epochSupplier = min -> min >= maxEpoch ? min : maxEpoch == 1 ? 1 : source.nextLong(min, maxEpoch);
            }
            final LongUnaryOperator hlcSupplier; {
                long maxHlc = source.nextLong(10, 1000000);
                hlcSupplier = min -> min >= maxHlc ? min : source.nextLong(min, maxHlc);
            }
            final Supplier<Node.Id> idSupplier; {
                int maxId = source.nextInt(1, 10);
                Int2ObjectHashMap<Node.Id> lookup = new Int2ObjectHashMap<>();
                idSupplier = () -> lookup.computeIfAbsent(maxId == 1 ? 1 : source.nextInt(1, maxId), Node.Id::new);
            }
            final IntSupplier flagSupplier = () -> 0;
            final Supplier<Txn.Kind> kindSupplier = () -> {
                float v = source.nextFloat();
                if (v < 0.5) return Txn.Kind.Read;
                if (v < 0.95) return Txn.Kind.Write;
                if (v < 0.97) return Txn.Kind.SyncPoint;
                return Txn.Kind.ExclusiveSyncPoint;
            };

            boolean permitMissing = source.decide(0.75f);
            final IntSupplier missingCountSupplier; {
                if (!permitMissing)
                {
                    missingCountSupplier = () -> 0;
                }
                else
                {
                    float zeroChance = source.nextFloat();
                    int maxMissing = source.nextInt(1, 10);
                    missingCountSupplier = () -> {
                        float v = source.nextFloat();
                        if (v < zeroChance) return 0;
                        return source.nextInt(0, maxMissing);
                    };
                }
            }

            // TODO (expected): we currently don't explore TruncatedApply statuses because we don't transition through all phases and therefore don't adopt the Applied status
            Choices<SaveStatus> saveStatusChoices = Choices.uniform(EnumSet.complementOf(EnumSet.of(SaveStatus.TruncatedApply, SaveStatus.TruncatedApplyWithOutcome, SaveStatus.TruncatedApplyWithDeps)).toArray(SaveStatus[]::new));
            Supplier<SaveStatus> saveStatusSupplier = () -> {
                SaveStatus result = saveStatusChoices.choose(source);
                while (result.is(Status.Truncated)) // we don't currently process truncations
                    result = saveStatusChoices.choose(source);
                return result;
            };

            Set<Timestamp> uniqueTs = new TreeSet<>();
            final Function<Timestamp, TxnId> txnIdSupplier = timestampSupplier(uniqueTs, txnIdSupplier(epochSupplier, hlcSupplier, kindSupplier, idSupplier));
            boolean permitExecuteAt = source.decide(0.75f);
            final Function<TxnId, Timestamp> executeAtSupplier;
            {
                if (!permitExecuteAt)
                {
                    executeAtSupplier = id -> id;
                }
                else
                {
                    Function<Timestamp, Timestamp> rawTimestampSupplier = timestampSupplier(uniqueTs, timestampSupplier(epochSupplier, hlcSupplier, flagSupplier, idSupplier));
                    float useTxnIdChance = source.nextFloat();
                    BooleanSupplier useTxnId = () -> source.decide(useTxnIdChance);
                    executeAtSupplier = txnId -> useTxnId.getAsBoolean() ? txnId : rawTimestampSupplier.apply(txnId);
                }
            }

            Supplier<Ballot> ballotSupplier;
            {
                Supplier<Ballot> delegate = ballotSupplier(epochSupplier, hlcSupplier, flagSupplier, idSupplier);
                ballotSupplier =  () -> source.decide(0.5f) ? Ballot.ZERO : delegate.get();
            }

            PartialTxn txn = createPartialTxn(0);
            RoutingKey key = ((Key) txn.keys().get(0)).toUnseekable();
            ObjectGraph graph = generateObjectGraph(source.nextInt(0, 100), () -> txnIdSupplier.apply(null), saveStatusSupplier, ignore -> txn, executeAtSupplier, ballotSupplier, missingCountSupplier, source);
            List<Command> commands = graph.toCommands();
            CommandsForKey cfk = new CommandsForKey(key);
            while (commands.size() > 0)
            {
                int next = source.nextInt(commands.size());
                Command command = commands.get(next);
                cfk = cfk.update(command).cfk();
                commands.set(next, commands.get(commands.size() - 1));
                commands.remove(commands.size() - 1);
            }

            for (int i = 0, j = 0 ; j < graph.cmds.length ; ++j)
            {
                Cmd cmd = graph.cmds[j];
                if (i >= cfk.size() || !cfk.txnId(i).equals(cmd.txnId))
                {
                    Assert.assertTrue(cmd.invisible);
                    continue;
                }
                TxnInfo info = cfk.get(i);
                InternalStatus expectStatus = InternalStatus.from(cmd.saveStatus);
                if (expectStatus == null) expectStatus = InternalStatus.TRANSITIVE;
                if (expectStatus.hasExecuteAtAndDeps)
                    Assert.assertEquals(cmd.executeAt, info.executeAt);
                Assert.assertEquals(expectStatus, info.status());
                Assert.assertArrayEquals(cmd.missing.toArray(TxnId[]::new), info.missing());
                if (expectStatus.hasBallot)
                    Assert.assertEquals(cmd.ballot, info.ballot());
                ++i;
            }

            ByteBuffer buffer = Serialize.toBytesWithoutKey(cfk);
            CommandsForKey roundTrip = Serialize.fromBytes(key, buffer);
            Assert.assertEquals(cfk, roundTrip);
        }
        catch (Throwable t)
        {
            throw new AssertionError(seed + " seed failed", t);
        }
    }

    @Test
    public void test()
    {
        var tableGen = AccordGenerators.fromQT(CassandraGenerators.TABLE_ID_GEN);
        var txnIdGen = AccordGens.txnIds(rs -> rs.nextLong(0, 100), rs -> rs.nextLong(100), rs -> rs.nextInt(10));
        qt().check(rs -> {
            TableId table = tableGen.next(rs);
            TokenKey pk = new TokenKey(table, new Murmur3Partitioner.LongToken(rs.nextLong()));
            var redudentBefore = txnIdGen.next(rs);
            TxnId[] ids = Gens.arrays(TxnId.class, rs0 -> {
                TxnId next = txnIdGen.next(rs0);
                while (next.compareTo(redudentBefore) <= 0)
                    next = txnIdGen.next(rs0);
                return next;
            }).unique().ofSizeBetween(0, 10).next(rs);
            Arrays.sort(ids, Comparator.naturalOrder());
            TxnInfo[] info = new TxnInfo[ids.length];
            InternalStatus[] statuses = Stream.of(InternalStatus.values()).filter(s -> s != InternalStatus.PRUNED).toArray(InternalStatus[]::new);
            for (int i = 0; i < info.length; i++)
            {
                InternalStatus status = rs.pick(statuses);
                info[i] = TxnInfo.create(ids[i], status, false, true, ids[i], TxnId.NO_TXNIDS, Ballot.ZERO);
            }

            Gen<Unmanaged.Pending> pendingGen = Gens.enums().allMixedDistribution(Unmanaged.Pending.class).next(rs);

            Unmanaged[] unmanaged = Gens.lists(txnIdGen)
                                        .unique()
                                        .ofSizeBetween(0, 10)
                                        .map((rs0, txnIds) -> txnIds.stream().map(i -> new Unmanaged(pendingGen.next(rs0), i, i)).toArray(Unmanaged[]::new))
                                        .next(rs);
            Arrays.sort(unmanaged, Comparator.naturalOrder());
            if (unmanaged.length > 0)
            {
                // when registering unmanaged, if the txn is "missing" in TxnInfo we add it
                List<TxnInfo> missing = new ArrayList<>(unmanaged.length);
                for (Unmanaged u : unmanaged)
                {
                    int idx = Arrays.binarySearch(ids, u.txnId);
                    if (idx < 0)
                        missing.add(TxnInfo.create(u.txnId, InternalStatus.TRANSITIVE, false, true, u.txnId, Ballot.ZERO));
                }
                if (!missing.isEmpty())
                {
                    info = ArrayUtils.addAll(info, missing.toArray(TxnInfo[]::new));
                    Arrays.sort(info, Comparator.naturalOrder());
                }
            }
            else unmanaged = CommandsForKey.NO_PENDING_UNMANAGED;

            CommandsForKey expected = CommandsForKey.SerializerSupport.create(pk, info, unmanaged, TxnId.NONE, NO_BOUNDS_INFO);

            ByteBuffer buffer = Serialize.toBytesWithoutKey(expected);
            CommandsForKey roundTrip = Serialize.fromBytes(pk, buffer);
            Assert.assertEquals(expected, roundTrip);
        });
    }

    @Test
    public void thereAndBackAgain()
    {
        long tokenValue = -2311778975040348869L;
        Token token = new Murmur3Partitioner.LongToken(tokenValue);
        TokenKey pk = new TokenKey(TableId.fromString("1b255f4d-ef25-40a6-0000-000000000009"), token);
        TxnId txnId = TxnId.fromValues(11,34052499,2,1);
        CommandsForKey expected = CommandsForKey.SerializerSupport.create(pk,
                                                     new TxnInfo[] { TxnInfo.create(txnId, InternalStatus.PREACCEPTED_OR_ACCEPTED_INVALIDATE, false, true, txnId, TxnId.NO_TXNIDS, Ballot.ZERO) },
                                                                          CommandsForKey.NO_PENDING_UNMANAGED, TxnId.NONE, NO_BOUNDS_INFO);

        ByteBuffer buffer = Serialize.toBytesWithoutKey(expected);
        CommandsForKey roundTrip = Serialize.fromBytes(pk, buffer);
        Assert.assertEquals(expected, roundTrip);
    }
}
