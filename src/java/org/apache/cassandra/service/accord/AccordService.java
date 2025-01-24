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

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.LocalConfig;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Barrier;
import accord.coordinate.Barrier.AsyncSyncPoint;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.CoordinationAdapter.Adapters.SyncPointAdapter;
import accord.coordinate.CoordinationFailed;
import accord.coordinate.ExecuteSyncPoint;
import accord.coordinate.Exhausted;
import accord.coordinate.FailureAccumulator;
import accord.coordinate.Invalidated;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.coordinate.TopologyMismatch;
import accord.coordinate.tracking.AllTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.impl.AbstractConfigurationService;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultRemoteListeners;
import accord.impl.DurabilityScheduling;
import accord.impl.RequestCallbacks;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.ShardDistributor.EvenSplit;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.Reply;
import accord.messages.Request;
import accord.messages.WaitUntilApplied;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.AccordSplitter;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.AccordClientRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordSyncPropagator.Notification;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.KeyspaceSplitter;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.apache.cassandra.service.accord.api.AccordTopologySorter;
import org.apache.cassandra.service.accord.api.CompositeTopologySorter;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.exceptions.ReadExhaustedException;
import org.apache.cassandra.service.accord.exceptions.ReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.WritePreemptedException;
import org.apache.cassandra.service.accord.interop.AccordInteropAdapter.AccordInteropFactory;
import org.apache.cassandra.service.accord.repair.RepairSyncPointAdapter;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TableMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.Blocking;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.messages.SimpleReply.Ok;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.utils.Invariants.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordCommandStoreShardCount;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordWriteMetrics;
import static org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState.maybeSaveAccordKeyMigrationLocally;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.getTableMetadata;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordService implements IAccordService, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordService.class);

    private enum State {INIT, STARTED, SHUTTING_DOWN, SHUTDOWN}

    private final Node node;
    private final Shutdownable nodeShutdown;
    private final AccordMessageSink messageSink;
    private final AccordConfigurationService configService;
    private final AccordFastPathCoordinator fastPathCoordinator;
    private final AccordScheduler scheduler;
    private final AccordDataStore dataStore;
    private final AccordJournal journal;
    private final AccordVerbHandler<? extends Request> requestHandler;
    private final AccordResponseVerbHandler<? extends Reply> responseHandler;
    private final LocalConfig configuration;

    @GuardedBy("this")
    private State state = State.INIT;

    private static final IAccordService NOOP_SERVICE = new NoOpAccordService();

    private static volatile IAccordService instance = null;

    @VisibleForTesting
    public static void unsafeSetNewAccordService(IAccordService service)
    {
        instance = service;
    }

    @VisibleForTesting
    public static void unsafeSetNoop()
    {
        instance = NOOP_SERVICE;
    }

    public static boolean isSetup()
    {
        return instance != null;
    }

    public static IVerbHandler<? extends Request> requestHandlerOrNoop()
    {
        if (!isSetup()) return ignore -> {};
        return instance().requestHandler();
    }

    public static IVerbHandler<? extends Reply> responseHandlerOrNoop()
    {
        if (!isSetup()) return ignore -> {};
        return instance().responseHandler();
    }

    public synchronized static void startup(NodeId tcmId)
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
        {
            instance = NOOP_SERVICE;
            return;
        }

        if (instance != null)
            return;

        AccordService as = new AccordService(AccordTopology.tcmIdToAccord(tcmId));
        as.startup();
        if (StorageService.instance.isReplacingSameAddress())
        {
            // when replacing another node but using the same ip the hostId will also match, this causes no TCM transactions
            // to be committed...
            // In order to bootup correctly, need to pull in the current epoch
            ClusterMetadata current = ClusterMetadata.current();
            as.configurationService().listener.notifyPostCommit(current, current, false);
        }
        instance = as;

        replayJournal(as);
    }

    @VisibleForTesting
    public static void replayJournal(AccordService as)
    {
        logger.info("Starting journal replay.");
        CommandsForKey.disableLinearizabilityViolationsReporting();
        try
        {
            AccordKeyspace.truncateAllCaches();
            as.journal().replay(as.node().commandStores());

            logger.info("Waiting for command stores to quiesce.");
            ((AccordCommandStores)as.node.commandStores()).waitForQuiescense();
            as.journal.unsafeSetStarted();
        }
        finally
        {
            CommandsForKey.enableLinearizabilityViolationsReporting();
        }

        logger.info("Finished journal replay.");
    }

    public static void shutdownServiceAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        IAccordService i = instance;
        if (i == null)
            return;
        i.shutdownAndWait(timeout, unit);
    }

    @Override
    public boolean shouldAcceptMessages()
    {
        return state == State.STARTED && journal.started();
    }

    public static IAccordService instance()
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
            return NOOP_SERVICE;
        IAccordService i = instance;
        Invariants.checkState(i != null, "AccordService was not started");
        return i;
    }

    public static boolean started()
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
            return false;
        return instance != null;
    }

    @VisibleForTesting
    public AccordService(Id localId)
    {
        Invariants.checkState(localId != null, "static localId must be set before instantiating AccordService");
        logger.info("Starting accord with nodeId {}", localId);
        AccordAgent agent = FBUtilities.construct(CassandraRelevantProperties.ACCORD_AGENT_CLASS.getString(AccordAgent.class.getName()), "AccordAgent");
        agent.setNodeId(localId);
        AccordTimeService time = new AccordTimeService();
        final RequestCallbacks callbacks = new RequestCallbacks(time);
        this.scheduler = new AccordScheduler();
        this.dataStore = new AccordDataStore();
        this.configuration = new AccordConfiguration(DatabaseDescriptor.getRawConfig());
        this.journal = new AccordJournal(DatabaseDescriptor.getAccord().journal, agent);
        this.configService = new AccordConfigurationService(localId, journal);
        this.fastPathCoordinator = AccordFastPathCoordinator.create(localId, configService);
        this.messageSink = new AccordMessageSink(agent, configService, callbacks);
        this.node = new Node(localId,
                             messageSink,
                             configService,
                             time,
                             () -> dataStore,
                             new KeyspaceSplitter(new EvenSplit<>(getAccordCommandStoreShardCount(), getPartitioner().accordSplitter())),
                             agent,
                             new DefaultRandom(),
                             scheduler,
                             CompositeTopologySorter.create(SizeOfIntersectionSorter.SUPPLIER,
                                                            new AccordTopologySorter.Supplier(configService, DatabaseDescriptor.getEndpointSnitch())),
                             DefaultRemoteListeners::new,
                             ignore -> callbacks,
                             DefaultProgressLogs::new,
                             DefaultLocalListeners.Factory::new,
                             AccordCommandStores.factory(),
                             new AccordInteropFactory(agent, configService),
                             journal.durableBeforePersister(),
                             configuration,
                             journal);
        this.nodeShutdown = toShutdownable(node);
        this.requestHandler = new AccordVerbHandler<>(node, configService);
        this.responseHandler = new AccordResponseVerbHandler<>(callbacks, configService);
    }

    @Override
    public synchronized void startup()
    {
        if (state != State.INIT)
            return;
        journal.start(node);
        node.load();

        ClusterMetadata metadata = ClusterMetadata.current();
        configService.updateMapping(metadata);

        // Load all active topologies, wihout writing them to journal again. No-op on bootstrap.
        node.commandStores().restoreShardStateUnsafe(topology -> configService.reportTopology(topology, true, true));
        configService.start();

        long minEpoch = fetchMinEpoch();
        if (minEpoch >= 0)
        {
            for (long epoch = minEpoch; epoch <= metadata.epoch.getEpoch(); epoch++)
                node.configService().fetchTopologyForEpoch(epoch);

            try
            {
                epochReady(metadata.epoch).get(DatabaseDescriptor.getTransactionTimeout(MILLISECONDS), MILLISECONDS);
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
            catch (ExecutionException | TimeoutException e)
            {
                throw new RuntimeException(e);
            }
        }

        fastPathCoordinator.start();
        ClusterMetadataService.instance().log().addListener(fastPathCoordinator);
        node.durabilityScheduling().setDefaultRetryDelay(Ints.checkedCast(DatabaseDescriptor.getAccordDefaultDurabilityRetryDelay(SECONDS)), SECONDS);
        node.durabilityScheduling().setMaxRetryDelay(Ints.checkedCast(DatabaseDescriptor.getAccordMaxDurabilityRetryDelay(SECONDS)), SECONDS);
        node.durabilityScheduling().setTargetShardSplits(Ints.checkedCast(DatabaseDescriptor.getAccordShardDurabilityTargetSplits()));
        node.durabilityScheduling().setGlobalCycleTime(Ints.checkedCast(DatabaseDescriptor.getAccordGlobalDurabilityCycle(SECONDS)), SECONDS);
        node.durabilityScheduling().setShardCycleTime(Ints.checkedCast(DatabaseDescriptor.getAccordShardDurabilityCycle(SECONDS)), SECONDS);
        node.durabilityScheduling().setTxnIdLag(Ints.checkedCast(DatabaseDescriptor.getAccordScheduleDurabilityTxnIdLag(SECONDS)), TimeUnit.SECONDS);
        node.durabilityScheduling().start();
        state = State.STARTED;
    }

    /**
     * Queries peers to discover min epoch
     */
    private long fetchMinEpoch()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        Map<InetAddressAndPort, Set<TokenRange>> peers = new HashMap<>();
        for (KeyspaceMetadata keyspace : metadata.schema.getKeyspaces())
        {
            List<TableMetadata> tables = keyspace.tables.stream().filter(TableMetadata::requiresAccordSupport).collect(Collectors.toList());
            if (tables.isEmpty())
                continue;
            DataPlacement current = metadata.placements.get(keyspace.params.replication);
            DataPlacement settled = metadata.writePlacementAllSettled(keyspace);
            Sets.SetView<InetAddressAndPort> alive = Sets.intersection(settled.writes.byEndpoint().keySet(), current.writes.byEndpoint().keySet());
            InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
            settled.writes.forEach((range, group) -> {
                if (group.endpoints().contains(self))
                {
                    for (InetAddressAndPort peer : group.endpoints())
                    {
                        if (peer.equals(self) || !alive.contains(peer)) continue;
                        for (TableMetadata table : tables)
                            peers.computeIfAbsent(peer, i -> new HashSet<>()).add(AccordTopology.fullRange(table.id));
                    }
                }
            });
        }
        if (peers.isEmpty())
            return -1;

        Long minEpoch = findMinEpoch(SharedContext.Global.instance, peers);
        if (minEpoch == null)
            return -1;
        return minEpoch;
    }

    @VisibleForTesting
    static Long findMinEpoch(SharedContext context, Map<InetAddressAndPort, Set<TokenRange>> peers)
    {
        try
        {
            return FetchMinEpoch.fetch(context, peers).get();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public IVerbHandler<? extends Request> requestHandler()
    {
        return requestHandler;
    }

    @Override
    public IVerbHandler<? extends Reply> responseHandler()
    {
        return responseHandler;
    }

    public DurabilityScheduling.ImmutableView durabilityScheduling()
    {
        return node.durabilityScheduling().immutableView();
    }

    private Seekables<?, ?> barrier(@Nonnull Seekables<?, ?> keysOrRanges, long epoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite, BiFunction<Node, FullRoute<?>, AsyncSyncPoint> syncPoint)
    {
        Stopwatch sw = Stopwatch.createStarted();
        keysOrRanges = intersectionWithAccordManagedRanges(keysOrRanges);
        // It's possible none of them were Accord managed and we aren't going to treat that as an error
        if (keysOrRanges.isEmpty())
        {
            logger.info("Skipping barrier because there are no ranges managed by Accord");
            return keysOrRanges;
        }

        FullRoute<?> route = node.computeRoute(epoch, keysOrRanges);
        AccordClientRequestMetrics metrics = isForWrite ? accordWriteMetrics : accordReadMetrics;
        try
        {
            logger.debug("Starting barrier key: {} epoch: {} barrierType: {} isForWrite {}", keysOrRanges, epoch, barrierType, isForWrite);
            AsyncResult<TxnId> asyncResult = syncPoint == null
                                                 ? Barrier.barrier(node, keysOrRanges, route, epoch, barrierType)
                                                 : Barrier.barrier(node, keysOrRanges, route, epoch, barrierType, syncPoint);
            long deadlineNanos = requestTime.startedAtNanos() + timeoutNanos;
            TxnId txnId = AsyncChains.getBlocking(asyncResult, deadlineNanos - nanoTime(), NANOSECONDS);
            if (keysOrRanges.domain() == Key)
            {
                PartitionKey key = (PartitionKey)keysOrRanges.get(0);
                maybeSaveAccordKeyMigrationLocally(key, Epoch.create(txnId.epoch()));
            }
            logger.debug("Completed barrier attempt in {}ms, {}ms since attempts start, barrier key: {} epoch: {} barrierType: {} isForWrite {}",
                         sw.elapsed(MILLISECONDS),
                         NANOSECONDS.toMillis(nanoTime() - requestTime.startedAtNanos()),
                         keysOrRanges, epoch, barrierType, isForWrite);
            return keysOrRanges;
        }
        catch (ExecutionException e)
        {
            Throwable cause = Throwables.getRootCause(e);
            if (cause instanceof Timeout)
            {
                TxnId txnId = ((Timeout) cause).txnId();
                ((AccordAgent) node.agent()).onFailedBarrier(txnId, keysOrRanges, cause);
                metrics.timeouts.mark();
                throw newBarrierTimeout(((CoordinationFailed)cause).txnId(), barrierType, isForWrite, keysOrRanges);
            }
            if (cause instanceof Preempted)
            {
                TxnId txnId = ((Preempted) cause).txnId();
                ((AccordAgent) node.agent()).onFailedBarrier(txnId, keysOrRanges, cause);
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                throw newBarrierPreempted(((CoordinationFailed)cause).txnId(), barrierType, isForWrite, keysOrRanges);
            }
            if (cause instanceof Exhausted)
            {
                TxnId txnId = ((Exhausted) cause).txnId();
                ((AccordAgent) node.agent()).onFailedBarrier(txnId, keysOrRanges, cause);
                // this case happens when a non-timeout exception is seen, and we are unable to move forward
                metrics.failures.mark();
                throw newBarrierExhausted(((CoordinationFailed)cause).txnId(), barrierType, isForWrite, keysOrRanges);
            }
            // unknown error
            metrics.failures.mark();
            throw new RuntimeException(cause);
        }
        catch (InterruptedException e)
        {
            metrics.failures.mark();
            throw new UncheckedInterruptedException(e);
        }
        catch (TimeoutException e)
        {
            metrics.timeouts.mark();
            throw newBarrierTimeout(null, barrierType, isForWrite, keysOrRanges);
        }
        finally
        {
            // TODO Should barriers have a dedicated latency metric? Should it be a read/write metric?
            // What about counts for timeouts/failures/preempts?
            metrics.addNano(nanoTime() - requestTime.startedAtNanos());
        }
    }

    @Override
    public Seekables<?, ?> barrier(@Nonnull Seekables<?, ?> keysOrRanges, long epoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite)
    {
        return barrier(keysOrRanges, epoch, requestTime, timeoutNanos, barrierType, isForWrite, null);
    }

    public static BiFunction<Node, FullRoute<?>, AsyncSyncPoint> repairSyncPoint(Set<Node.Id> allNodes)
    {
        return (node, route) -> {
            TxnId txnId = node.nextTxnId(Kind.SyncPoint, route.domain());
            AsyncResult<SyncPoint<?>> async = CoordinateSyncPoint.coordinate(node, Kind.SyncPoint, route, (SyncPointAdapter)RepairSyncPointAdapter.create(allNodes));
            return new AsyncSyncPoint(txnId, async);
        };
    }

    @Override
    public Seekables<?, ?> repair(@Nonnull Seekables<?, ?> keysOrRanges, long epoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints)
    {
        Set<Node.Id> allNodes = allEndpoints.stream().map(configService::mappedId).collect(Collectors.toUnmodifiableSet());
        return barrier(keysOrRanges, epoch, requestTime, timeoutNanos, barrierType, isForWrite, repairSyncPoint(allNodes));
    }

    private static Seekables<?, ?> intersectionWithAccordManagedRanges(Seekables<?, ?> keysOrRanges)
    {
        TableId tableId = null;
        for (Seekable keyOrRange : keysOrRanges)
        {
            TableId newTableId;
            if (keysOrRanges.domain() == Key)
                newTableId = ((PartitionKey)keyOrRange).table();
            else if (keysOrRanges.domain() == Range)
                newTableId = ((TokenRange) keyOrRange).table();
            else
                throw new IllegalStateException("Unexpected domain " + keysOrRanges.domain());

            if (tableId == null)
                tableId = newTableId;
            else if (!tableId.equals(newTableId))
                throw new IllegalArgumentException("Currently only one table is handled here.");
        }

        ClusterMetadata cm = ClusterMetadata.current();
        TableMetadata tm = getTableMetadata(cm, tableId);

        // Barriers can be needed just because it's an Accord managed range, but it could also be a migration back to Paxos
        // in which case we do want to barrier the migrating/migrated ranges even though the target for the migration is not Accord
        // In either case Accord should be aware of those ranges and not generate a topology mismatch
        if (tm.params.transactionalMode != TransactionalMode.off || tm.params.transactionalMigrationFrom.from != TransactionalMode.off)
        {
            TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tm.id);
            // null is fine could be completely migrated or was always an Accord table on creation
            if (tms == null)
                return keysOrRanges;
            Ranges migratingAndMigratedRanges = AccordTopology.toAccordRanges(tms.tableId, tms.migratingAndMigratedRanges);
            return keysOrRanges.slice(migratingAndMigratedRanges);
        }

        switch (keysOrRanges.domain())
        {
            case Key:
                return Keys.EMPTY;
            case Range:
                return Ranges.EMPTY;
            default:
                throw new IllegalStateException("Only keys and ranges are supported");
        }
    }

    @VisibleForTesting
    static ReadTimeoutException newBarrierTimeout(@Nonnull TxnId txnId, BarrierType barrierType, boolean isForWrite, Seekables<?, ?> keysOrRanges)
    {
        return new ReadTimeoutException(barrierType.global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, String.format("Timeout waiting on barrier %s / %s / %s; impacted ranges %s", txnId, barrierType, isForWrite ? "write" : "not write", keysOrRanges));
    }

    @VisibleForTesting
    static ReadTimeoutException newBarrierPreempted(@Nullable TxnId txnId, BarrierType barrierType, boolean isForWrite, Seekables<?, ?> keysOrRanges)
    {
        return new ReadPreemptedException(barrierType.global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, String.format("Preempted waiting on barrier %s / %s / %s; impacted ranges %s", txnId, barrierType, isForWrite ? "write" : "not write", keysOrRanges));
    }

    @VisibleForTesting
    static ReadExhaustedException newBarrierExhausted(@Nullable TxnId txnId, BarrierType barrierType, boolean isForWrite, Seekables<?, ?> keysOrRanges)
    {
        return new ReadExhaustedException(barrierType.global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, String.format("Exhausted (too many failures from peers) waiting on barrier %s / %s / %s; impacted ranges %s", txnId, barrierType, isForWrite ? "write" : "not write", keysOrRanges));
    }

    @VisibleForTesting
    static boolean isTimeout(Throwable t)
    {
        return t instanceof Timeout || t instanceof ReadTimeoutException || t instanceof Preempted || t instanceof ReadPreemptedException;
    }

    @VisibleForTesting
    static Seekables doWithRetries(Blocking blocking, Supplier<Seekables> action, int retryAttempts, long initialBackoffMillis, long maxBackoffMillis) throws InterruptedException
    {
        // Since we could end up having the barrier transaction or the transaction it listens to invalidated
        Throwable existingFailures = null;
        Seekables success = null;
        long backoffMillis = initialBackoffMillis;
        for (int attempt = 0; attempt < retryAttempts; attempt++)
        {
            try
            {
                success = action.get();
                break;
            }
            catch (TopologyMismatch topologyMismatch)
            {
                // Retry topology mismatch immediately because we should be able calculate the correct ranges immediately
                backoffMillis = 0;
            }
            catch (RequestExecutionException | CoordinationFailed newFailures)
            {
                logger.error("Had failure on barrier", newFailures);
                existingFailures = FailureAccumulator.append(existingFailures, newFailures, AccordService::isTimeout);

                try
                {
                    blocking.sleep(backoffMillis);
                }
                catch (InterruptedException e)
                {
                    if (existingFailures != null)
                        e.addSuppressed(existingFailures);
                    throw e;
                }
                backoffMillis = Math.min(backoffMillis * 2, maxBackoffMillis);
            }
            catch (Throwable t)
            {
                // if an unknown/unexpected error happens retry stops right away
                if (existingFailures != null)
                    t.addSuppressed(existingFailures);
                existingFailures = t;
                break;
            }
        }
        if (success == null)
        {
            logger.error("Ran out of retries for barrier");
            checkState(existingFailures != null, "Didn't have success, but also didn't have failures");
            Throwables.throwIfUnchecked(existingFailures);
            throw new RuntimeException(existingFailures);
        }
        return success;
    }

    @Override
    public Seekables barrierWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException
    {
        return doWithRetries(Blocking.Default.instance, () -> AccordService.instance().barrier(keysOrRanges, minEpoch, Dispatcher.RequestTime.forImmediateExecution(), DatabaseDescriptor.getAccordRangeSyncPointTimeoutNanos(), barrierType, isForWrite),
                             DatabaseDescriptor.getAccordBarrierRetryAttempts(),
                             DatabaseDescriptor.getAccordBarrierRetryInitialBackoffMillis(),
                             DatabaseDescriptor.getAccordBarrierRetryMaxBackoffMillis());
    }

    @Override
    public Seekables<?, ?> repairWithRetries(Seekables<?, ?> keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints) throws InterruptedException
    {
        return doWithRetries(Blocking.Default.instance, () -> AccordService.instance().repair(keysOrRanges, minEpoch, Dispatcher.RequestTime.forImmediateExecution(), DatabaseDescriptor.getAccordRangeSyncPointTimeoutNanos(), barrierType, isForWrite, allEndpoints),
                             DatabaseDescriptor.getAccordBarrierRetryAttempts(),
                             DatabaseDescriptor.getAccordBarrierRetryInitialBackoffMillis(),
                             DatabaseDescriptor.getAccordBarrierRetryMaxBackoffMillis());
    }

    @Override
    public long currentEpoch()
    {
        return configService.currentEpoch();
    }

    @Override
    public TopologyManager topology()
    {
        return node.topology();
    }

    /**
     * Consistency level is just echoed back in timeouts, in the future it may be used for interoperability
     * with non-Accord operations.
     */
    @Override
    public @Nonnull TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull Dispatcher.RequestTime requestTime)
    {
        AsyncTxnResult asyncTxnResult = coordinateAsync(minEpoch, txn, consistencyLevel, requestTime);
        return getTxnResult(asyncTxnResult, txn.isWrite(), consistencyLevel, requestTime);
    }

    @Override
    public @Nonnull AsyncTxnResult coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull Dispatcher.RequestTime requestTime)
    {
        TxnId txnId = node.nextTxnId(txn.kind(), txn.keys().domain());
        ClientRequestMetrics sharedMetrics;
        AccordClientRequestMetrics metrics;
        if (txn.isWrite())
        {
            sharedMetrics = ClientRequestsMetricsHolder.writeMetrics;
            metrics = accordWriteMetrics;
        }
        else
        {
            sharedMetrics = ClientRequestsMetricsHolder.readMetrics;
            metrics = accordReadMetrics;
        }
        metrics.keySize.update(txn.keys().size());
        long deadlineNanos = requestTime.computeDeadline(DatabaseDescriptor.getTransactionTimeout(NANOSECONDS));
        AsyncResult<Result> asyncResult = node.coordinate(txnId, txn, minEpoch, deadlineNanos);
        AsyncTxnResult asyncTxnResult = new AsyncTxnResult(txnId);
        asyncResult.addCallback((success, failure) -> {
            long durationNanos = nanoTime() - requestTime.startedAtNanos();
            sharedMetrics.addNano(durationNanos);
            metrics.addNano(durationNanos);
            Throwable cause = failure != null ? Throwables.getRootCause(failure) : null;
            if (success != null)
            {
                if (((TxnResult) success).kind() == TxnResult.Kind.retry_new_protocol)
                {
                    metrics.retryDifferentSystem.mark();
                    Tracing.trace("Got retry different system error from Accord, will retry");
                }
                asyncTxnResult.trySuccess((TxnResult) success);
                return;
            }

            if (cause instanceof Timeout)
            {
                // Don't mark the metric here, should be done in getTxnResult to ensure it only happens once
                // since both Accord and the thread blocked on the result can trigger a timeout
                asyncTxnResult.tryFailure(newTimeout(txnId, txn.isWrite(), consistencyLevel));
                return;
            }
            if (cause instanceof Preempted || cause instanceof Invalidated)
            {
                sharedMetrics.timeouts.mark();
                metrics.preempted.mark();
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                asyncTxnResult.tryFailure(newPreempted(txnId, txn.isWrite(), consistencyLevel));
                return;
            }
            sharedMetrics.failures.mark();
            if (cause instanceof TopologyMismatch)
            {
                metrics.topologyMismatches.mark();
                asyncTxnResult.tryFailure(RequestValidations.invalidRequest(cause.getMessage()));
                return;
            }
            metrics.failures.mark();
            asyncTxnResult.tryFailure(new RuntimeException(cause));
        });
        return asyncTxnResult;
    }

    @Override
    public TxnResult getTxnResult(AsyncTxnResult asyncTxnResult, boolean isWrite, @Nullable ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        ClientRequestMetrics sharedMetrics;
        AccordClientRequestMetrics metrics;
        if (isWrite)
        {
            sharedMetrics = ClientRequestsMetricsHolder.writeMetrics;
            metrics = accordWriteMetrics;
        }
        else
        {
            sharedMetrics = ClientRequestsMetricsHolder.readMetrics;
            metrics = accordReadMetrics;
        }
        try
        {
            long deadlineNanos = requestTime.computeDeadline(DatabaseDescriptor.getTransactionTimeout(NANOSECONDS));
            TxnResult result = asyncTxnResult.get(deadlineNanos - nanoTime(), NANOSECONDS);
            return result;
        }
        catch (ExecutionException e)
        {
            // Metrics except timeout have already been handled
            Throwable cause = e.getCause();
            if (cause instanceof RequestTimeoutException)
            {
                // Mark here instead of in coordinate async since this is where the request timeout actually occurs
                metrics.timeouts.mark();
                sharedMetrics.timeouts.mark();
                cause.addSuppressed(e);
                throw (RequestTimeoutException) cause;
            }
            else if (cause instanceof RuntimeException)
                throw (RuntimeException) cause;
            else
                throw new RuntimeException(cause);
        }
        catch (InterruptedException e)
        {
            metrics.failures.mark();
            sharedMetrics.failures.mark();
            throw new UncheckedInterruptedException(e);
        }
        catch (TimeoutException e)
        {
            metrics.timeouts.mark();
            sharedMetrics.timeouts.mark();
            throw newTimeout(asyncTxnResult.txnId, isWrite, consistencyLevel);
        }
    }

    private static RequestTimeoutException newTimeout(TxnId txnId, boolean isWrite, ConsistencyLevel consistencyLevel)
    {
        // Client protocol doesn't handle null consistency level so use ANY
        if (consistencyLevel == null)
            consistencyLevel = ConsistencyLevel.ANY;
        return isWrite ? new WriteTimeoutException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                       : new ReadTimeoutException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    private static RuntimeException newPreempted(TxnId txnId, boolean isWrite, ConsistencyLevel consistencyLevel)
    {
        if (consistencyLevel == null)
            consistencyLevel = ConsistencyLevel.ANY;
        return isWrite ? new WritePreemptedException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                       : new ReadPreemptedException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    @Override
    public void setCacheSize(long kb)
    {
        long bytes = kb << 10;
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        commandStores.setCapacity(bytes);
    }

    @Override
    public void setWorkingSetSize(long kb)
    {
        long bytes = kb << 10;
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        commandStores.setWorkingSetSize(bytes);
    }

    @Override
    public boolean isTerminated()
    {
        return scheduler.isTerminated();
    }

    @Override
    public synchronized void shutdown()
    {
        if (state != State.STARTED)
            return;
        state = State.SHUTTING_DOWN;
        shutdownAndWait(1, TimeUnit.MINUTES);
        state = State.SHUTDOWN;
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        try
        {
            ExecutorUtils.awaitTermination(timeout, units, shutdownableSubsystems());
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    private List<Shutdownable> shutdownableSubsystems()
    {
        return Arrays.asList(scheduler, nodeShutdown, journal, configService);
    }

    @VisibleForTesting
    @Override
    public void shutdownAndWait(long timeout, TimeUnit unit)
    {
        if (!ExecutorUtils.shutdownSequentiallyAndWait(shutdownableSubsystems(), timeout, unit))
            logger.error("One or more subsystems did not shut down cleanly.");
    }

    @Override
    public AccordScheduler scheduler()
    {
        return scheduler;
    }

    public Id nodeId()
    {
        return node.id();
    }

    @Override
    public List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId)
    {
        AsyncChain<List<CommandStoreTxnBlockedGraph>> states = loadDebug(txnId);
        try
        {
            return AsyncChains.getBlocking(states);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    public AsyncChain<List<CommandStoreTxnBlockedGraph>> loadDebug(TxnId original)
    {
        CommandStores commandStores = node.commandStores();
        if (commandStores.count() == 0)
            return AsyncChains.success(Collections.emptyList());
        int[] ids = commandStores.ids();
        List<AsyncChain<CommandStoreTxnBlockedGraph>> chains = new ArrayList<>(ids.length);
        for (int id : ids)
            chains.add(loadDebug(original, commandStores.forId(id)));
        return AsyncChains.all(chains);
    }

    private AsyncChain<CommandStoreTxnBlockedGraph> loadDebug(TxnId txnId, CommandStore store)
    {
        CommandStoreTxnBlockedGraph.Builder state = new CommandStoreTxnBlockedGraph.Builder(store.id());
        return populate(state, store, txnId).map(ignore -> state.build());
    }

    private static AsyncChain<Void> populate(CommandStoreTxnBlockedGraph.Builder state, CommandStore store, TxnId txnId)
    {
        AsyncChain<AsyncChain<Void>> submit = store.submit(PreLoadContext.contextFor(txnId), in -> {
            AsyncChain<Void> chain = populate(state, (AccordSafeCommandStore) in, txnId);
            return chain == null ? AsyncChains.success(null) : chain;
        });
        return submit.flatMap(Function.identity());
    }

    private static AsyncChain<Void> populate(CommandStoreTxnBlockedGraph.Builder state, CommandStore commandStore, TokenKey blockedBy, TxnId txnId, Timestamp executeAt)
    {
        AsyncChain<AsyncChain<Void>> submit = commandStore.submit(PreLoadContext.contextFor(txnId, RoutingKeys.of(blockedBy.toUnseekable()), KeyHistory.SYNC), in -> {
            AsyncChain<Void> chain = populate(state, (AccordSafeCommandStore) in, blockedBy, txnId, executeAt);
            return chain == null ? AsyncChains.success(null) : chain;
        });
        return submit.flatMap(Function.identity());
    }

    @Nullable
    private static AsyncChain<Void> populate(CommandStoreTxnBlockedGraph.Builder state, AccordSafeCommandStore safeStore, TxnId txnId)
    {
        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
        Invariants.nonNull(safeCommand, "Txn %s is not in the cache", txnId);
        if (safeCommand.current() == null || safeCommand.current().saveStatus() == SaveStatus.Uninitialised)
            return null;
        CommandStoreTxnBlockedGraph.TxnState cmdTxnState = populate(state, safeCommand.current());
        if (cmdTxnState.notBlocked())
            return null;
        //TODO (safety): check depth
        List<AsyncChain<Void>> chains = new ArrayList<>();
        for (TxnId blockedBy : cmdTxnState.blockedBy)
        {
            if (state.knows(blockedBy)) continue;
            // need to fetch the state
            if (safeStore.ifLoadedAndInitialised(blockedBy) != null)
            {
                AsyncChain<Void> chain = populate(state, safeStore, blockedBy);
                if (chain != null)
                    chains.add(chain);
            }
            else
            {
                // go fetch it
                chains.add(populate(state, safeStore.commandStore(), blockedBy));
            }
        }
        for (TokenKey blockedBy : cmdTxnState.blockedByKey)
        {
            if (state.keys.containsKey(blockedBy)) continue;
            if (safeStore.ifLoadedAndInitialised(blockedBy) != null)
            {
                AsyncChain<Void> chain = populate(state, safeStore, blockedBy, txnId, safeCommand.current().executeAt());
                if (chain != null)
                    chains.add(chain);
            }
            else
            {
                // go fetch it
                chains.add(populate(state, safeStore.commandStore(), blockedBy, txnId, safeCommand.current().executeAt()));
            }
        }
        if (chains.isEmpty())
            return null;
        return AsyncChains.all(chains).map(ignore -> null);
    }

    private static AsyncChain<Void> populate(CommandStoreTxnBlockedGraph.Builder state, AccordSafeCommandStore safeStore, TokenKey pk, TxnId txnId, Timestamp executeAt)
    {
        SafeCommandsForKey commandsForKey = safeStore.ifLoadedAndInitialised(pk);
        TxnId blocking = commandsForKey.current().blockedOnTxnId(txnId, executeAt);
        if (blocking instanceof CommandsForKey.TxnInfo)
            blocking = ((CommandsForKey.TxnInfo) blocking).plainTxnId();
        state.keys.put(pk, blocking);
        if (state.txns.containsKey(blocking)) return null;
        if (safeStore.ifLoadedAndInitialised(blocking) != null) return populate(state, safeStore, blocking);
        return populate(state, safeStore.commandStore(), blocking);
    }

    private static CommandStoreTxnBlockedGraph.TxnState populate(CommandStoreTxnBlockedGraph.Builder state, Command cmd)
    {
        CommandStoreTxnBlockedGraph.Builder.TxnBuilder cmdTxnState = state.txn(cmd.txnId(), cmd.executeAt(), cmd.saveStatus());
        if (!cmd.hasBeen(Status.Applied) && cmd.hasBeen(Status.Stable))
        {
            // check blocking state
            Command.WaitingOn waitingOn = cmd.asCommitted().waitingOn();
            waitingOn.waitingOn.reverseForEach(null, null, null, null, (i1, i2, i3, i4, i) -> {
                if (i < waitingOn.txnIdCount())
                {
                    // blocked on txn
                    cmdTxnState.blockedBy.add(waitingOn.txnId(i));
                }
                else
                {
                    // blocked on key
                    cmdTxnState.blockedByKey.add((TokenKey) waitingOn.keys.get(i - waitingOn.txnIdCount()));
                }
            });
        }
        return cmdTxnState.build();
    }

    @Nullable
    @Override
    public Long minEpoch(Collection<TokenRange> ranges)
    {
        return node.topology().minEpoch();
    }

    @Override
    public void tryMarkRemoved(Topology topology, Id target)
    {
        if (node.commandStores().count() == 0) return; // when starting up stores can be empty, so ignore
        Ranges ranges = topology.rangesForNode(target);
        if (ranges.isEmpty()) return;
        long startNanos = Clock.Global.nanoTime();
        exclusiveSyncPointWithRetries(ranges, 0)
        .begin((s, f) -> {
            if (f != null)
            {
                logger.warn("Unable to mark the ranges for {} as durable after node left; took {}", target, Duration.ofNanos(Clock.Global.nanoTime() - startNanos), f);
                node.agent().onUncaughtException(f);
            }
            else
            {
                logger.info("Marked {} ranges as durable after node left; took {}", target, Duration.ofNanos(Clock.Global.nanoTime() - startNanos));
            }
        });
    }

    private AsyncChain<SyncPoint<accord.primitives.Range>> exclusiveSyncPointWithRetries(Ranges ranges, int attempt)
    {
        return CoordinateSyncPoint.exclusiveSyncPoint(node, ranges)
                                  .recover(t ->
                                           //TODO (operability): make this configurable / monitorable?
                                           attempt <= 3 && t instanceof Invalidated || t instanceof Preempted || t instanceof Timeout ? exclusiveSyncPointWithRetries(ranges, attempt + 1) : null);
    }

    public Node node()
    {
        return node;
    }

    public AccordJournal journal()
    {
        return journal;
    }

    @Override
    public Future<Void> epochReady(Epoch epoch)
    {
        AsyncPromise<Void> promise = new AsyncPromise<>();
        AsyncChain<Void> ready = configService.epochReady(epoch.getEpoch());
        ready.begin((result, failure) -> {
            if (failure == null) promise.trySuccess(result);
            else promise.tryFailure(failure);
        });
        return promise;
    }

    @Override
    public void receive(Message<List<Notification>> message)
    {
        receive(MessagingService.instance(), configService, message);
    }

    @VisibleForTesting
    public static void receive(MessageDelivery sink, AbstractConfigurationService<?, ?> configService, Message<List<Notification>> message)
    {
        List<AccordSyncPropagator.Notification> notifications = message.payload;
        notifications.forEach(notification -> {
            notification.syncComplete.forEach(id -> configService.receiveRemoteSyncComplete(id, notification.epoch));
            if (!notification.closed.isEmpty())
                configService.receiveClosed(notification.closed, notification.epoch);
            if (!notification.redundant.isEmpty())
                configService.receiveRedundant(notification.redundant, notification.epoch);
        });
        sink.respond(Ok, message);
    }

    private static Shutdownable toShutdownable(Node node)
    {
        return new Shutdownable() {
            private volatile boolean isShutdown = false;

            @Override
            public boolean isTerminated()
            {
                // we don't know about terminiated... so settle for shutdown!
                return isShutdown;
            }

            @Override
            public void shutdown()
            {
                isShutdown = true;
                node.shutdown();
            }

            @Override
            public Object shutdownNow()
            {
                // node doesn't offer shutdownNow
                shutdown();
                return null;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit units)
            {
                // TODO (required): expose awaitTermination in Node
                // node doesn't offer
                return true;
            }
        };
    }

    @VisibleForTesting
    public AccordConfigurationService configurationService()
    {
        return configService;
    }

    @Override
    public CompactionInfo getCompactionInfo()
    {
        Int2ObjectHashMap<RedundantBefore> redundantBefores = new Int2ObjectHashMap<>();
        Int2ObjectHashMap<DurableBefore> durableBefores = new Int2ObjectHashMap<>();
        Int2ObjectHashMap<RangesForEpoch> ranges = new Int2ObjectHashMap<>();
        if (node.commandStores().all().length > 0)
        {
            AsyncChains.getBlockingAndRethrow(node.commandStores().forEach(safeStore -> {
                synchronized (redundantBefores)
                {
                    redundantBefores.put(safeStore.commandStore().id(), safeStore.redundantBefore());
                    ranges.put(safeStore.commandStore().id(), safeStore.ranges());
                    durableBefores.put(safeStore.commandStore().id(), safeStore.durableBefore());
                }
            }));
        }
        return new CompactionInfo(redundantBefores, ranges, durableBefores);
    }

    @Override
    public AccordAgent agent()
    {
        return (AccordAgent) node.agent();
    }

    @Override
    public void awaitTableDrop(TableId id)
    {
        // Need to make sure no existing txn are still being processed for this table... this is only used by DROP TABLE so NEW txn are expected to be blocked, so just need to "wait" for existing ones to complete
        Topology topology = node.topology().current();
        List<TokenRange> ranges = topology.reduce(new ArrayList<>(),
                                                  s -> ((TokenRange) s.range).table().equals(id),
                                                  (accum, s) -> {
                                                      accum.add((TokenRange) s.range);
                                                      return accum;
                                                  });
        if (ranges.isEmpty()) return; // nothing to see here

        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(id);
        Invariants.checkState(cfs != null, "Unable to find table %s", id);
        BigInteger targetSplitSize = BigInteger.valueOf(Math.max(1, cfs.estimateKeys() / 1_000_000));

        List<AsyncChain<?>> syncs = new ArrayList<>(ranges.size());
        for (TokenRange range : ranges)
            syncs.add(awaitTableDrop(cfs, range, targetSplitSize));
        AsyncChain<Object[]> all = AsyncChains.allOf(syncs);
        try
        {
            AsyncChains.getBlocking(all);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Params journalConfiguration()
    {
        return journal.configuration();
    }

    private AsyncChain<?> awaitTableDrop(ColumnFamilyStore cfs, TokenRange range, BigInteger targetSplitSize)
    {
        List<TokenRange> splits = split(cfs, range, targetSplitSize);
        List<AsyncChain<?>> syncs = new ArrayList<>(splits.size());
        for (TokenRange tr : splits)
            syncs.add(awaitTableDropSubRange(tr));
        return AsyncChains.allOf(syncs);
    }

    private List<TokenRange> split(ColumnFamilyStore cfs, TokenRange range, BigInteger targetSplitSize)
    {
        if (targetSplitSize.equals(BigInteger.ONE)) return Collections.singletonList(range);

        AccordSplitter splitter = cfs.getPartitioner().accordSplitter().apply(Ranges.single(range));
        RoutingKey remainingStart = range.start();

        BigInteger rangeSize = splitter.sizeOf(range);
        BigInteger divide = splitter.divide(rangeSize, targetSplitSize);
        BigInteger rangeStep = divide.equals(BigInteger.ZERO) ? rangeSize : BigInteger.ONE.max(divide);
        BigInteger offset = BigInteger.ZERO;
        List<TokenRange> result = new ArrayList<>();

        while (splitter.compare(offset, rangeSize) < 0)
        {
            BigInteger remaining = rangeSize.subtract(offset);
            BigInteger length = remaining.min(rangeStep);

            TokenRange next = splitter.subRange(range, offset, splitter.add(offset, length));
            result.add(next);
            remainingStart = next.end();
            offset = offset.add(length);
        }

        if (!remainingStart.equals(range.end()))
            result.add(range.newRange(remainingStart, range.end()));
        assert result.get(0).start().equals(range.start()) : String.format("Starting range %s does not have the same start as %s", result.get(0), range);
        assert result.get(result.size() - 1).end().equals(range.end()) : String.format("Ending range %s does not have the same end as %s", result.get(result.size() - 1), range);
        return result;
    }

    private AsyncChain<?> awaitTableDropSubRange(TokenRange range)
    {
        return awaitTableDropSubRange(Ranges.single(range), 0);
    }

    private AsyncChain<Void> awaitTableDropSubRange(Ranges ranges, int attempt)
    {
        return exclusiveSyncPoint(ranges, attempt)
               .flatMap(s -> s == null ? AsyncChains.success(null) : Await.coordinate(node, s));
    }

    private AsyncChain<SyncPoint<accord.primitives.Range>> exclusiveSyncPoint(Ranges ranges, int attempt)
    {
        //TODO (on merge): CASSANDRA-19769 has the same logic... should this be refactored?  Would make it nice so we could split the range on retries?
        return CoordinateSyncPoint.exclusiveSyncPoint(node, ranges)
                                  .recover(t -> {
                                      //TODO (operability): make this configurable / monitorable?
                                      if (attempt > 3) return null;
                                      switch (shouldRetry(t))
                                      {
                                          case SUCCESS:
                                              return AsyncChains.success(null);
                                          case RETRY:
                                              return exclusiveSyncPoint(ranges, attempt + 1);
                                          case FAIL:
                                              return null;
                                          default:
                                              throw new UnsupportedOperationException();
                                      }
                                  });
    }

    private enum RetryDecission { SUCCESS, RETRY, FAIL }
    private static RetryDecission shouldRetry(Throwable t)
    {
        if (t.getClass() == ExecuteSyncPoint.SyncPointErased.class)
            return RetryDecission.SUCCESS;
        if (t instanceof Invalidated || t instanceof Preempted || t instanceof Timeout)
            return RetryDecission.RETRY;
        return RetryDecission.FAIL;
    }

    // TODO (required): this should use ExecuteSyncPoint.ExecuteExclusive, or perhaps should not exist at all (should have some mechanism to request and await durability)
    private static class Await extends AsyncResults.SettableResult<SyncPoint<?>> implements Callback<ReadData.ReadReply>
    {
        private final Node node;
        private final AllTracker tracker;
        private final SyncPoint<?> exclusiveSyncPoint;

        private Await(Node node, SyncPoint<?> exclusiveSyncPoint)
        {
            Topologies topologies = node.topology().forEpoch(exclusiveSyncPoint.route, exclusiveSyncPoint.syncId.epoch());
            this.node = node;
            this.tracker = new AllTracker(topologies);
            this.exclusiveSyncPoint = exclusiveSyncPoint;
        }

        public static AsyncChain<Void> coordinate(Node node, SyncPoint<?> sp)
        {
            return node.withEpoch(sp.syncId.epoch(), () -> {
                Await coordinate = new Await(node, sp);
                coordinate.start();
                AsyncChain<Void> chain = coordinate.map(i -> null);
                return chain.recover(t -> {
                    switch (shouldRetry(t))
                    {
                        case SUCCESS: return AsyncChains.success(null);
                        case RETRY: return coordinate(node, sp);
                        case FAIL: return null;
                        default: throw new UnsupportedOperationException();
                    }
                });
            });
        }

        private void start()
        {
            node.send(tracker.nodes(), to -> new WaitUntilApplied(to, tracker.topologies(), exclusiveSyncPoint.syncId, exclusiveSyncPoint.route, exclusiveSyncPoint.syncId.epoch()), this);
        }
        @Override
        public void onSuccess(Node.Id from, ReadData.ReadReply reply)
        {
            if (!reply.isOk())
            {
                ReadData.CommitOrReadNack nack = (ReadData.CommitOrReadNack) reply;
                switch (nack)
                {
                    default: throw new AssertionError("Unhandled: " + reply);

                    case Insufficient:
                        CoordinateSyncPoint.sendApply(node, from, exclusiveSyncPoint);
                        return;
                    case Rejected:
                        tryFailure(new RuntimeException(nack.name()));
                    case Redundant:
                        tryFailure(new ExecuteSyncPoint.SyncPointErased());
                        return;
                }
            }
            else
            {
                if (tracker.recordSuccess(from) == RequestStatus.Success)
                {
                    node.configService().reportEpochRedundant(exclusiveSyncPoint.route.toRanges(), exclusiveSyncPoint.syncId.epoch() - 1);
                    trySuccess(exclusiveSyncPoint);
                }
            }
        }

        private Throwable cause;

        @Override
        public void onFailure(Node.Id from, Throwable failure)
        {
            synchronized (this)
            {
                if (cause == null) cause = failure;
                else
                {
                    try
                    {
                        cause.addSuppressed(failure);
                    }
                    catch (Throwable t)
                    {
                        // can not always add suppress
                        node.agent().onUncaughtException(failure);
                    }
                }
                failure = cause;
            }
            if (tracker.recordFailure(from) == RequestStatus.Failed)
                tryFailure(failure);
        }

        @Override
        public boolean onCallbackFailure(Node.Id from, Throwable failure)
        {
            return tryFailure(failure);
        }
    }
}
