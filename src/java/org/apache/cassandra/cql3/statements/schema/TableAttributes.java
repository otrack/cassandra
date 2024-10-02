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
package org.apache.cassandra.cql3.statements.schema;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.TableParams.Option;
import org.apache.cassandra.service.accord.fastpath.FastPathStrategy;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static java.lang.String.format;
import static org.apache.cassandra.schema.TableParams.Option.ADDITIONAL_WRITE_POLICY;
import static org.apache.cassandra.schema.TableParams.Option.ALLOW_AUTO_SNAPSHOT;
import static org.apache.cassandra.schema.TableParams.Option.BLOOM_FILTER_FP_CHANCE;
import static org.apache.cassandra.schema.TableParams.Option.CACHING;
import static org.apache.cassandra.schema.TableParams.Option.CDC;
import static org.apache.cassandra.schema.TableParams.Option.COMMENT;
import static org.apache.cassandra.schema.TableParams.Option.COMPACTION;
import static org.apache.cassandra.schema.TableParams.Option.COMPRESSION;
import static org.apache.cassandra.schema.TableParams.Option.CRC_CHECK_CHANCE;
import static org.apache.cassandra.schema.TableParams.Option.DEFAULT_TIME_TO_LIVE;
import static org.apache.cassandra.schema.TableParams.Option.GC_GRACE_SECONDS;
import static org.apache.cassandra.schema.TableParams.Option.INCREMENTAL_BACKUPS;
import static org.apache.cassandra.schema.TableParams.Option.MAX_INDEX_INTERVAL;
import static org.apache.cassandra.schema.TableParams.Option.MEMTABLE_FLUSH_PERIOD_IN_MS;
import static org.apache.cassandra.schema.TableParams.Option.MIN_INDEX_INTERVAL;
import static org.apache.cassandra.schema.TableParams.Option.READ_REPAIR;
import static org.apache.cassandra.schema.TableParams.Option.SPECULATIVE_RETRY;
import static org.apache.cassandra.schema.TableParams.Option.TRANSACTIONAL_MODE;

public final class TableAttributes extends PropertyDefinitions
{
    public static final String ID = "id";
    private static final Set<String> validKeywords;
    private static final Set<String> obsoleteKeywords;

    static
    {
        ImmutableSet.Builder<String> validBuilder = ImmutableSet.builder();
        for (Option option : Option.values())
            validBuilder.add(option.toString());
        validBuilder.add(ID);
        validKeywords = validBuilder.build();
        obsoleteKeywords = ImmutableSet.of();
    }

    public void validate()
    {
        validate(validKeywords, obsoleteKeywords);
        build(TableParams.builder()).validate();
    }

    TableParams asNewTableParams(String keyspaceName)
    {
        TableParams.Builder builder = TableParams.builder();
        if (!hasOption(TRANSACTIONAL_MODE) && !SchemaConstants.isSystemKeyspace(keyspaceName) && Schema.instance.distributedKeyspaces().names().contains(keyspaceName))
            builder.transactionalMode(DatabaseDescriptor.defaultTransactionalMode());
        return build(builder);
    }

    TableParams asAlteredTableParams(TableParams previous)
    {
        if (getId() != null)
            throw new ConfigurationException("Cannot alter table id.");
        return build(previous.unbuild());
    }

    public TableId getId() throws ConfigurationException
    {
        String id = getString(ID);
        try
        {
            return id != null ? TableId.fromString(id) : null;
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Invalid table id", e);
        }
    }

    public static Set<String> validKeywords()
    {
        return ImmutableSet.copyOf(validKeywords);
    }

    public static Set<String> allKeywords()
    {
        return Sets.union(validKeywords, obsoleteKeywords);
    }

    private TableParams build(TableParams.Builder builder)
    {
        if (hasOption(ALLOW_AUTO_SNAPSHOT))
            builder.allowAutoSnapshot(getBoolean(ALLOW_AUTO_SNAPSHOT.toString(), true));

        if (hasOption(BLOOM_FILTER_FP_CHANCE))
            builder.bloomFilterFpChance(getDouble(BLOOM_FILTER_FP_CHANCE));

        if (hasOption(CACHING))
            builder.caching(CachingParams.fromMap(getMap(CACHING)));

        if (hasOption(COMMENT))
            builder.comment(getString(COMMENT));

        if (hasOption(COMPACTION))
            builder.compaction(CompactionParams.fromMap(getMap(COMPACTION)));

        if (hasOption(COMPRESSION))
            builder.compression(CompressionParams.fromMap(getMap(COMPRESSION)));

        if (hasOption(Option.MEMTABLE))
            builder.memtable(MemtableParams.getWithFallback(getString(Option.MEMTABLE)));

        if (hasOption(DEFAULT_TIME_TO_LIVE))
            builder.defaultTimeToLive(getInt(DEFAULT_TIME_TO_LIVE));

        if (hasOption(GC_GRACE_SECONDS))
            builder.gcGraceSeconds(getInt(GC_GRACE_SECONDS));
        
        if (hasOption(INCREMENTAL_BACKUPS))
            builder.incrementalBackups(getBoolean(INCREMENTAL_BACKUPS.toString(), true));

        if (hasOption(MAX_INDEX_INTERVAL))
            builder.maxIndexInterval(getInt(MAX_INDEX_INTERVAL));

        if (hasOption(MEMTABLE_FLUSH_PERIOD_IN_MS))
            builder.memtableFlushPeriodInMs(getInt(MEMTABLE_FLUSH_PERIOD_IN_MS));

        if (hasOption(MIN_INDEX_INTERVAL))
            builder.minIndexInterval(getInt(MIN_INDEX_INTERVAL));

        if (hasOption(SPECULATIVE_RETRY))
            builder.speculativeRetry(SpeculativeRetryPolicy.fromString(getString(SPECULATIVE_RETRY)));

        if (hasOption(ADDITIONAL_WRITE_POLICY))
            builder.additionalWritePolicy(SpeculativeRetryPolicy.fromString(getString(ADDITIONAL_WRITE_POLICY)));

        if (hasOption(CRC_CHECK_CHANCE))
            builder.crcCheckChance(getDouble(CRC_CHECK_CHANCE));

        if (hasOption(CDC))
            builder.cdc(getBoolean(CDC));

        if (hasOption(READ_REPAIR))
            builder.readRepair(ReadRepairStrategy.fromString(getString(READ_REPAIR)));

        if (hasOption(Option.FAST_PATH))
        {
            try
            {
                builder.fastPath(FastPathStrategy.fromMap(getMap(Option.FAST_PATH)));
            }
            catch (SyntaxException e)
            {
                builder.fastPath(FastPathStrategy.tableStrategyFromString(getString(Option.FAST_PATH)));
            }
        }

        if (hasOption(Option.TRANSACTIONAL_MODE))
            builder.transactionalMode(TransactionalMode.fromString(getString(Option.TRANSACTIONAL_MODE)));

        if (hasOption(Option.TRANSACTIONAL_MIGRATION_FROM))
            builder.transactionalMigrationFrom(TransactionalMigrationFromMode.fromString(getString(Option.TRANSACTIONAL_MIGRATION_FROM)));

        return builder.build();
    }

    public boolean hasOption(Option option)
    {
        return hasProperty(option.toString());
    }

    private String getString(Option option)
    {
        String value = getString(option.toString());
        if (value == null)
            throw new IllegalStateException(format("Option '%s' is absent", option));
        return value;
    }

    private Map<String, String> getMap(Option option)
    {
        Map<String, String> value = getMap(option.toString());
        if (value == null)
            throw new IllegalStateException(format("Option '%s' is absent", option));
        return value;
    }

    private boolean getBoolean(Option option)
    {
        return parseBoolean(option.toString(), getString(option));
    }

    private int getInt(Option option)
    {
        return parseInt(option.toString(), getString(option));
    }

    private double getDouble(Option option)
    {
        return parseDouble(option.toString(), getString(option));
    }
}
