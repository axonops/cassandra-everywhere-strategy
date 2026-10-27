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
package com.axonops.everywhere.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/** Unit tests for the nodetool and cqlsh output parsing in {@link Shell}; no containers needed. */
public class ShellTest {

    private static final List<String> CASSANDRA = Arrays.asList("172.30.0.20", "172.30.0.21");

    private static String describecluster(String... versionLines) {
        StringBuilder sb = new StringBuilder("Cluster Information:\n"
                + "\tName: everywhere-it\n"
                + "\tSnitch: org.apache.cassandra.locator.DynamicEndpointSnitch\n"
                + "\tDynamicEndPointSnitch: enabled\n"
                + "\tPartitioner: org.apache.cassandra.dht.Murmur3Partitioner\n"
                + "\tSchema versions:\n");
        for (String line : versionLines) {
            sb.append("\t\t").append(line).append("\n\n");
        }
        return sb.toString();
    }

    @Test
    public void parsesEveryVersionAndIgnoresTheHeader() {
        Map<String, List<String>> v = Shell.schemaVersions(describecluster(
                "a1b2c3d4-0000-0000-0000-000000000001: [172.30.0.20, 172.30.0.21]",
                "a1b2c3d4-0000-0000-0000-000000000002: [172.30.0.10]"));
        assertEquals(2, v.size());
        assertEquals(CASSANDRA, v.get("a1b2c3d4-0000-0000-0000-000000000001"));
        assertEquals(Arrays.asList("172.30.0.10"), v.get("a1b2c3d4-0000-0000-0000-000000000002"));
    }

    @Test
    public void cassandraNodesAgreeWhenSharingOneVersionBesideDse() {
        assertTrue(Shell.cassandraAgree(Shell.schemaVersions(describecluster(
                "v1: [172.30.0.10]",
                "v2: [172.30.0.21, 172.30.0.20]")), CASSANDRA));
    }

    @Test
    public void cassandraNodesDisagreeWhenSplitAcrossVersions() {
        assertFalse(Shell.cassandraAgree(Shell.schemaVersions(describecluster(
                "v1: [172.30.0.10, 172.30.0.20]",
                "v2: [172.30.0.21]")), CASSANDRA));
    }

    @Test
    public void cassandraNodesDisagreeWhenOneIsUnreachable() {
        assertFalse(Shell.cassandraAgree(Shell.schemaVersions(describecluster(
                "v2: [172.30.0.20, 172.30.0.21]",
                "UNREACHABLE: [172.30.0.21]")), CASSANDRA));
    }

    @Test
    public void unreachableDseDoesNotBlockCassandraAgreement() {
        assertTrue(Shell.cassandraAgree(Shell.schemaVersions(describecluster(
                "UNREACHABLE: [172.30.0.10]",
                "v2: [172.30.0.20, 172.30.0.21]")), CASSANDRA));
    }

    @Test
    public void reportsUnreachableAsItsOwnEntry() {
        Map<String, List<String>> v = Shell.schemaVersions(describecluster(
                "v1: [172.30.0.20]", "UNREACHABLE: [172.30.0.21]"));
        assertEquals(2, v.size());
        assertTrue(v.containsKey("UNREACHABLE"));
    }

    private static final String STATUS = "Datacenter: dc1\n"
            + "===============\n"
            + "Status=Up/Down\n"
            + "|/ State=Normal/Leaving/Joining/Moving\n"
            + "--  Address      Load       Tokens       Owns (effective)  Host ID                               Rack\n"
            + "DN  172.30.0.10  ?          1            100.0%            5f1c2b3a-1111-2222-3333-444455556666  r1\n"
            + "Datacenter: dc2\n"
            + "===============\n"
            + "Status=Up/Down\n"
            + "|/ State=Normal/Leaving/Joining/Moving\n"
            + "--  Address      Load       Tokens       Owns (effective)  Host ID                               Rack\n"
            + "UN  172.30.0.20  245.31 KiB  256          100.0%            0a0b0c0d-aaaa-bbbb-cccc-000000000020  r1\n"
            + "UJ  172.30.0.21  98.1 KiB   256          100.0%            0a0b0c0d-aaaa-bbbb-cccc-000000000021  r1\n";

    @Test
    public void parsesNodeStatesAcrossDatacenters() {
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("172.30.0.10", "DN");
        expected.put("172.30.0.20", "UN");
        expected.put("172.30.0.21", "UJ");
        assertEquals(expected, Shell.nodeStates(STATUS));
    }

    @Test
    public void findsTheHostIdOfADownNode() {
        assertEquals("5f1c2b3a-1111-2222-3333-444455556666", Shell.hostId(STATUS, "172.30.0.10"));
        assertEquals("0a0b0c0d-aaaa-bbbb-cccc-000000000021", Shell.hostId(STATUS, "172.30.0.21"));
    }

    @Test(expected = AssertionError.class)
    public void hostIdOfAnUnknownAddressFails() {
        Shell.hostId(STATUS, "172.30.0.99");
    }

    @Test
    public void parsesCqlshRowsAfterTheConsistencyLine() {
        String out = "Consistency level set to ONE.\n"
                + "\n"
                + " keyspace_name | replication\n"
                + "---------------+------------------------------------------------------------\n"
                + "   system_auth | {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'}\n"
                + "    dse_system | {'class': 'org.apache.cassandra.locator.EverywhereStrategy'}\n"
                + "\n"
                + "(2 rows)\n";
        List<List<String>> rows = Shell.cqlRows(out);
        assertEquals(2, rows.size());
        assertEquals(Arrays.asList("system_auth",
                "{'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'}"), rows.get(0));
        assertEquals("dse_system", rows.get(1).get(0));
    }

    @Test
    public void parsesAnEmptyCqlshResult() {
        String out = "Consistency level set to ONE.\n\n k | v\n---+---\n\n(0 rows)\n";
        assertEquals(Collections.emptyList(), Shell.cqlRows(out));
    }
}
