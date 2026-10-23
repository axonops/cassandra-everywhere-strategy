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
import java.util.List;
import java.util.Map;

import org.junit.Test;

/** Unit tests for the {@code nodetool describecluster} parsing in {@link Shell}; no containers needed. */
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
}
