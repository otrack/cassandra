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

import accord.api.Journal;
import accord.local.Command;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.TxnId;
import accord.utils.PersistentField.Persister;

public interface IJournal extends Journal
{
    // TODO (required): migrate to accord.api.Journal
    default SavedCommand.MinimalCommand loadMinimal(int commandStoreId, TxnId txnId, SavedCommand.Load load, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        Command command = loadCommand(commandStoreId, txnId, redundantBefore, durableBefore);
        if (command == null)
            return null;
        return new SavedCommand.MinimalCommand(command.txnId(), command.saveStatus(), command.participants(), command.durability(), command.executeAt(), command.writes());
    }

    Persister<DurableBefore, DurableBefore> durableBeforePersister();
}