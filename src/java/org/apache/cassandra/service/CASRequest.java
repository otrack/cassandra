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
package org.apache.cassandra.service;

import accord.primitives.Txn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.paxos.Ballot;

import static org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult;

/**
 * Abstract the conditions and updates for a CAS operation.
 */
public interface CASRequest
{
    /**
     * The command to use to fetch the value to compare for the CAS.
     */
    SinglePartitionReadCommand readCommand(long nowInSec);

    /**
     * Returns whether the provided CF, that represents the values fetched using the
     * readFilter(), match the CAS conditions this object stands for.
     */
    boolean appliesTo(FilteredPartition current) throws InvalidRequestException;

    /**
     * The updates to perform of a CAS success. The values fetched using the readFilter()
     * are passed as argument.
     */
    PartitionUpdate makeUpdates(FilteredPartition current, ClientState clientState, Ballot ballot) throws InvalidRequestException;

    Txn toAccordTxn(ConsistencyLevel consistencyLevel, ClientState clientState, long nowInSecs);

    ConsensusAttemptResult toCasResult(TxnResult txnResult);
}
