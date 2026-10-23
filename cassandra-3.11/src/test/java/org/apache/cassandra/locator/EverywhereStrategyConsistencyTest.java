/*
 * Copyright 2026 AxonOps
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.locator;

import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Pins the consistency-level block counts computed by Cassandra's own code against EverywhereStrategy. */
public class EverywhereStrategyConsistencyTest {
    @BeforeClass
    public static void setUp() {
        Ring.initCassandra();
    }

    private static Keyspace keyspaceFor(Ring ring) {
        EverywhereStrategy strategy =
            new EverywhereStrategy("ks", ring.tm(), Ring.SNITCH, Collections.<String, String>emptyMap());
        Keyspace ks = mock(Keyspace.class);
        when(ks.getReplicationStrategy()).thenReturn(strategy);
        return ks;
    }

    @Test
    public void eachQuorumSumsPerDcQuorums() {
        // (3/2+1) + (2/2+1)
        assertEquals(4, ConsistencyLevel.EACH_QUORUM.blockFor(keyspaceFor(Ring.standard())));
    }

    @Test
    public void quorumUsesTotalNodeCount() {
        assertEquals(3, ConsistencyLevel.QUORUM.blockFor(keyspaceFor(Ring.standard())));
    }

    @Test
    public void allUsesTotalNodeCount() {
        assertEquals(5, ConsistencyLevel.ALL.blockFor(keyspaceFor(Ring.standard())));
    }

    @Test
    public void blockForTracksMembership() {
        Ring ring = Ring.standard();
        Keyspace ks = keyspaceFor(ring);
        ring.node("127.0.1.3", "dc2", "r1", 250);
        // dc1: 3/2+1 = 2, dc2: 3/2+1 = 2
        assertEquals(4, ConsistencyLevel.EACH_QUORUM.blockFor(ks));
        assertEquals(6, ConsistencyLevel.ALL.blockFor(ks));
    }
}
