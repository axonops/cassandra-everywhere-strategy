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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.locator.Ring.ip;
import static org.apache.cassandra.locator.Ring.t;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EverywhereStrategyPlacementTest {
    private static final String[] STANDARD_IPS = {
        "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.1.1", "127.0.1.2"
    };

    @BeforeClass
    public static void setUp() {
        Ring.initCassandra();
    }

    private static EverywhereStrategy strategy(Ring ring) {
        return strategy(ring.tm());
    }

    private static EverywhereStrategy strategy(TokenMetadata tm) {
        return new EverywhereStrategy("ks", tm, Ring.SNITCH, Collections.<String, String>emptyMap());
    }

    private static List<InetAddress> ips(String... values) {
        List<InetAddress> out = new ArrayList<>();
        for (String v : values) {
            out.add(ip(v));
        }
        return out;
    }

    @Test
    public void everyNormalNodeIsAReplicaInRingOrderFromThePrimaryOwner() {
        Ring ring = Ring.standard();
        // token 60 -> first token >= 60 is 100 (127.0.0.2)
        assertEquals(ips("127.0.0.2", "127.0.1.2", "127.0.0.3", "127.0.0.1", "127.0.1.1"),
                     strategy(ring).calculateNaturalEndpoints(t(60), ring.tm()));
    }

    @Test
    public void vnodesDoNotProduceDuplicates() {
        Ring ring = new Ring()
            .node("127.0.0.1", "dc1", "r1", 0, 300)
            .node("127.0.0.2", "dc1", "r1", 100)
            .node("127.0.0.3", "dc1", "r2", 200)
            .node("127.0.1.1", "dc2", "r1", 50)
            .node("127.0.1.2", "dc2", "r1", 150);
        List<InetAddress> replicas = strategy(ring).calculateNaturalEndpoints(t(250), ring.tm());
        assertEquals(5, replicas.size());
        assertEquals(5, new HashSet<>(replicas).size());
        assertEquals(ip("127.0.0.1"), replicas.get(0));
    }

    @Test
    public void replicationFactorsArePerDcNodeCounts() {
        EverywhereStrategy s = strategy(Ring.standard());
        assertEquals(3, s.getReplicationFactor("dc1"));
        assertEquals(2, s.getReplicationFactor("dc2"));
        assertEquals(5, s.getReplicationFactor());
        assertEquals(new HashSet<>(Arrays.asList("dc1", "dc2")), s.getDatacenters());
    }

    @Test
    public void replicationFactorsTrackMembership() {
        Ring ring = Ring.standard();
        EverywhereStrategy s = strategy(ring);
        ring.node("127.0.1.3", "dc2", "r1", 175);
        assertEquals(3, s.getReplicationFactor("dc2"));
        assertEquals(6, s.getReplicationFactor());
        ring.remove("127.0.1.1");
        assertEquals(2, s.getReplicationFactor("dc2"));
        assertEquals(5, s.getReplicationFactor());
    }

    @Test
    public void cachedNaturalEndpointsSeeANewNode() {
        Ring ring = Ring.standard();
        EverywhereStrategy s = strategy(ring);
        assertEquals(5, s.getNaturalEndpoints(t(60)).size());
        ring.node("127.0.1.3", "dc2", "r1", 175);
        assertEquals(6, s.getNaturalEndpoints(t(60)).size());
    }

    @Test
    public void bootstrappingNodeIsNotANaturalReplica() {
        Ring ring = Ring.standard().bootstrap("127.0.1.3", "dc2", "r1", 175);
        EverywhereStrategy s = strategy(ring);
        List<InetAddress> replicas = s.calculateNaturalEndpoints(t(60), ring.tm());
        assertFalse(replicas.contains(ip("127.0.1.3")));
        assertEquals(5, replicas.size());
        assertEquals(2, s.getReplicationFactor("dc2"));
    }

    @Test
    public void bootstrappingNodeGetsTheWholeRingAsPending() {
        Ring ring = Ring.standard().bootstrap("127.0.1.3", "dc2", "r1", 175);
        TokenMetadata tm = ring.tm();
        tm.calculatePendingRanges(strategy(ring), "ks");
        assertEquals(6, tm.getPendingRanges("ks", ip("127.0.1.3")).size());
    }

    @Test
    public void leavingNodeCreatesNoPendingRanges() {
        Ring ring = Ring.standard().leave("127.0.0.3");
        assertNoPendingRanges(ring);
    }

    @Test
    public void movingNodeCreatesNoPendingRanges() {
        Ring ring = Ring.standard().move("127.0.0.2", 120);
        assertNoPendingRanges(ring);
    }

    private static void assertNoPendingRanges(Ring ring) {
        TokenMetadata tm = ring.tm();
        tm.calculatePendingRanges(strategy(ring), "ks");
        for (String x : STANDARD_IPS) {
            assertTrue(x + " has pending ranges", tm.getPendingRanges("ks", ip(x)).isEmpty());
        }
    }

    @Test
    public void usesTheTokenMetadataItIsGiven() {
        EverywhereStrategy s = strategy(Ring.standard());
        Ring other = new Ring()
            .node("127.0.0.1", "dc1", "r1", 0)
            .node("127.0.1.1", "dc2", "r1", 50);
        assertEquals(2, s.calculateNaturalEndpoints(t(0), other.tm()).size());
    }

    @Test
    public void emptyRingIsSafe() {
        TokenMetadata tm = new TokenMetadata();
        EverywhereStrategy s = strategy(tm);
        assertEquals(0, s.getReplicationFactor());
        assertEquals(0, s.getReplicationFactor("dc1"));
        assertTrue(s.getDatacenters().isEmpty());
        assertTrue(s.calculateNaturalEndpoints(t(0), tm).isEmpty());
    }

    @Test
    public void replicationFactorsWorkOnTheLiveNodeTokenMetadata() {
        // On a running node the strategy holds StorageService's own TokenMetadata, whose getTopology()
        // asserts it is not called directly (Cassandra runs with -ea by default).
        EverywhereStrategy s = strategy(StorageService.instance.getTokenMetadata());
        assertEquals(0, s.getReplicationFactor());
        assertEquals(0, s.getReplicationFactor("dc1"));
        assertTrue(s.getDatacenters().isEmpty());
    }

    @Test
    public void unknownOrNullDcHasReplicationFactorZero() {
        EverywhereStrategy s = strategy(Ring.standard());
        assertEquals(0, s.getReplicationFactor("nope"));
        assertEquals(0, s.getReplicationFactor(null));
    }
}
