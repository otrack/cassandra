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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.junit.Assert.assertEquals;

public class MetadataTest
{
    @Test
    public void testUpdate()
    {
        Metadata metadata = Metadata.create();

        metadata.update();
        metadata.update();
        metadata.update();
        metadata.update();

        assertEquals(4, metadata.totalCount());
    }

    @Test
    public void testWriteRead() throws IOException
    {
        Metadata metadata = Metadata.create();

        metadata.update();
        metadata.update();
        metadata.update();
        metadata.update();

        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            metadata.write(out);
            ByteBuffer serialized = out.buffer();

            try (DataInputBuffer in = new DataInputBuffer(serialized, false))
            {
                Metadata deserialized = Metadata.read(in);
                assertEquals(4, deserialized.totalCount());
            }
        }
    }
}
