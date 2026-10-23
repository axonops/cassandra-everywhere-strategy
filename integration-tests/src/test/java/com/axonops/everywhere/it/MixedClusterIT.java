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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;

/**
 * The migration path end to end on one shared cluster: one DSE 5.1 node (dc1) and two Apache Cassandra 3.11
 * nodes with the EverywhereStrategy jar (dc2). The methods run in name order and each builds on the state the
 * previous one left behind, so a failure early on makes the later ones fail too; read the first failure.
 *
 * <p>While DSE is in the ring {@code nodetool describecluster} always shows two schema versions (one for DSE,
 * one shared by the Cassandra nodes), so schema agreement is checked among the Cassandra nodes only until DSE
 * has been removed in {@link #t10_postMigrationWithoutTheJar()}.
 *
 * <p>The setup follows the runbook's add-DC order: all DDL on DSE first, {@code system_auth} moved to
 * NetworkTopologyStrategy with dc1 only before any Cassandra node joins (so the joining nodes own none of it),
 * then the joins, then dc2 added to {@code system_auth} and rebuilt on each Cassandra node straight away.
 *
 * <p>Credentials are test-only. The {@code cassandra} superuser is used only on DSE before the Cassandra nodes
 * join, because it logs in at QUORUM; everything afterwards runs as {@code migrator}.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MixedClusterIT {

    private static final String SUBNET = "172.30.0";
    private static final String DSE_IP = SUBNET + ".10";
    private static final String C0_IP = SUBNET + ".20";
    private static final String C1_IP = SUBNET + ".21";
    private static final List<String> CASSANDRA_IPS = Arrays.asList(C0_IP, C1_IP);

    private static final String USER = "migrator";
    private static final String PASS = "migrator";
    private static final String EVERYWHERE = "org.apache.cassandra.locator.EverywhereStrategy";
    private static final String MISSING_STRATEGY = "Unable to find replication strategy class";
    private static final String MERGE_FAILURE = "Unable to merge schema";
    private static final String STRATEGY_IN_USE = "EverywhereStrategy in use for keyspace";
    private static final Pattern REPLICATION_CLASS = Pattern.compile("'class': '([^']+)'");

    /**
     * Keyspaces whose dc1 data the Cassandra nodes rebuild in t02 (Ruling H: a whole-node rebuild fails on
     * dse_perf). {@code system_auth} is rebuilt earlier, in the setup, straight after dc2 is added to it.
     */
    private static final List<String> REBUILD_KEYSPACES =
            Arrays.asList("everywhere_it", "dse_system", "dse_insights");

    /** Written at LOCAL_QUORUM via DSE in t05 and read again after every disruptive step. */
    private static final String KEY = "lq-dse";
    private static final String VALUE = "from-dse";

    private static final Duration SHORT = Duration.ofMinutes(2);
    private static final Duration LONG = Duration.ofMinutes(5);

    private static MixedCluster cluster;

    @BeforeClass
    public static void startClusterAndJoinCassandra() {
        cluster = MixedCluster.builder()
                .subnet(SUBNET)
                .cassandraNodes(2)
                .withJar(true)
                .build()
                .start();
        GenericContainer<?> dse = cluster.dse();

        // On DSE, before any Cassandra node joins, as the default superuser.
        Shell.cql(dse, "cassandra", "cassandra", "ONE",
                "CREATE ROLE migrator WITH PASSWORD = 'migrator' AND LOGIN = true AND SUPERUSER = true");
        Shell.cql(dse, "cassandra", "cassandra", "ONE",
                "CREATE KEYSPACE everywhere_it WITH replication = {'class': '" + EVERYWHERE + "'}");
        Shell.cql(dse, "cassandra", "cassandra", "ONE",
                "CREATE TABLE everywhere_it.kv (k text PRIMARY KEY, v text)");
        Shell.cql(dse, "cassandra", "cassandra", "ONE",
                "INSERT INTO everywhere_it.kv (k, v) VALUES ('seed', 'seeded-on-dse')");
        // Before any join: SimpleStrategy -> NTS with dc1 only, so the joining nodes take none of its ranges.
        // Last statement as 'cassandra'.
        Shell.cql(dse, "cassandra", "cassandra", "ONE",
                "ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1}");

        cluster.joinCassandra(0);
        cluster.joinCassandra(1);
        Shell.awaitNodeStates(dse, states("UN", "UN", "UN"), LONG);
        for (GenericContainer<?> node : cluster.cassandra()) {
            Shell.awaitNodeStates(node, states("UN", "UN", "UN"), LONG);
        }

        // Only now add dc2 to system_auth, as migrator (LOCAL_ONE login, served by DSE), and rebuild it at once,
        // one node at a time, before anyone logs in on a Cassandra node.
        Shell.cql(dse, USER, PASS, "ONE",
                "ALTER KEYSPACE system_auth WITH replication = "
                        + "{'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 2}");
        for (GenericContainer<?> node : cluster.cassandra()) {
            // A rebuild run before the node has the new replication would exit 0 and stream nothing.
            awaitEndpoints(node, "system_auth", "roles", USER, DSE_IP, C0_IP, C1_IP);
        }
        for (GenericContainer<?> node : cluster.cassandra()) {
            System.out.println("nodetool rebuild -ks system_auth -- " + MixedCluster.DSE_DC + " on "
                    + cluster.ip(node));
            Shell.nodetool(node, "rebuild", "-ks", "system_auth", "--", MixedCluster.DSE_DC);
        }
    }

    @AfterClass
    public static void closeCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void t01_cassandraNodesJoinAndAgreeOnSchema() {
        Shell.awaitNodeStates(dse(), states("UN", "UN", "UN"), LONG);
        Shell.awaitCassandraSchemaAgreement(c0(), CASSANDRA_IPS, SHORT);
        Shell.awaitLog(c0(), STRATEGY_IN_USE + " dse_system", SHORT);
        Shell.awaitLog(c1(), STRATEGY_IN_USE + " dse_system", SHORT);
        assertNoSchemaErrors();
        // The keyspace sets and logins on the Cassandra nodes are checked in t03, after t02's rebuilds.
    }

    @Test
    public void t02_rebuildFromDseDcSucceeds() {
        for (GenericContainer<?> node : cluster.cassandra()) {
            for (String ks : REBUILD_KEYSPACES) {
                System.out.println("nodetool rebuild -ks " + ks + " -- " + MixedCluster.DSE_DC + " on "
                        + cluster.ip(node));
                Shell.nodetool(node, "rebuild", "-ks", ks, "--", MixedCluster.DSE_DC);
            }
        }
    }

    @Test
    public void t03_roleCreatedOnDseLogsInOnCassandra() {
        for (GenericContainer<?> node : cluster.cassandra()) {
            Shell.cql(node, USER, PASS, "ONE", "SELECT now() FROM system.local");
        }
        // The row DSE held before the Cassandra nodes joined was streamed by the rebuild.
        assertRead(c0(), "LOCAL_QUORUM", "seed", "seeded-on-dse");
        assertSameKeyspacesAsDse(c0());
        assertSameKeyspacesAsDse(c1());
    }

    @Test
    public void t04_everyNodeIsAReplica() {
        assertEquals(new TreeSet<>(Arrays.asList(DSE_IP, C0_IP, C1_IP)),
                endpoints(c0(), "everywhere_it", "kv", "somekey"));
    }

    @Test
    public void t05_readsAndWritesAtEveryConsistencyLevel() {
        write(dse(), "LOCAL_QUORUM", KEY, VALUE);
        assertRead(c0(), "LOCAL_QUORUM", KEY, VALUE);

        write(c1(), "LOCAL_QUORUM", "lq-cassandra-1", "from-cassandra-1");
        assertRead(dse(), "LOCAL_QUORUM", "lq-cassandra-1", "from-cassandra-1");

        for (String cl : Arrays.asList("QUORUM", "EACH_QUORUM", "ALL")) {
            String key = cl.toLowerCase();
            write(c0(), cl, key, "at-" + key);
            assertRead(c0(), cl, key, "at-" + key);
        }
    }

    @Test
    public void t05b_localQuorumUsesThePerDcReplicationFactor() {
        // dc2 has RF 2 for an EverywhereStrategy keyspace, so LOCAL_QUORUM there needs both Cassandra nodes;
        // a global count (3 nodes, quorum 2, two of them up) would wrongly let it through.
        cluster.stop(c1());
        Shell.awaitNodeStates(c0(), states("UN", "UN", "DN"), SHORT);

        ExecResult r = Shell.cqlRaw(c0(), USER, PASS, "LOCAL_QUORUM",
                "SELECT v FROM everywhere_it.kv WHERE k = '" + KEY + "'");
        String out = r.getStdout() + r.getStderr();
        System.out.println("LOCAL_QUORUM read with one of two dc2 nodes down: exit " + r.getExitCode() + "\n" + out);
        assertNotEquals("a LOCAL_QUORUM read with one of two dc2 nodes down must fail", 0, r.getExitCode());
        assertTrue("expected Unavailable, got:\n" + out, out.contains("Unavailable"));
        assertRead(c0(), "LOCAL_ONE", KEY, VALUE);

        cluster.restart(c1());
        Shell.awaitNodeStates(c0(), states("UN", "UN", "UN"), LONG);
        Shell.awaitNodeStates(c1(), states("UN", "UN", "UN"), LONG);
        Shell.awaitNodeStates(dse(), states("UN", "UN", "UN"), LONG);
        Shell.awaitCqlLogin(c1(), USER, PASS, SHORT);
        assertRead(c1(), "LOCAL_QUORUM", KEY, VALUE);
    }

    @Test
    public void t06_repairFromCassandraNodeSucceeds() {
        // The output is long (~20 KB); only its verdict matters.
        String out = Shell.nodetool(c0(), "repair", "-full", "everywhere_it");
        assertTrue("repair did not report success; last 2000 characters:\n"
                + out.substring(Math.max(0, out.length() - 2000)), out.contains("Repair completed successfully"));
    }

    @Test
    public void t07_dseRestartDoesNotBreakCassandra() {
        Shell.cql(c0(), USER, PASS, "ONE", "ALTER KEYSPACE dse_system WITH replication = "
                + "{'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 2}");
        assertTrue(replication(c0(), "dse_system").contains("NetworkTopologyStrategy"));

        cluster.restart(dse());
        Shell.awaitCqlLogin(dse(), USER, PASS, LONG);

        // DSE 5.1 puts dse_system back on EverywhereStrategy when it starts; the Cassandra nodes must accept it.
        awaitReplication(c0(), "dse_system", EVERYWHERE, SHORT);
        Shell.awaitCassandraSchemaAgreement(c0(), CASSANDRA_IPS, SHORT);
        assertEquals(EVERYWHERE, replicationClass(c1(), "dse_system"));
        Shell.awaitNodeStates(c0(), states("UN", "UN", "UN"), SHORT);
        Shell.awaitNodeStates(dse(), states("UN", "UN", "UN"), SHORT);
        assertSameKeyspacesAsDse(c0());
        assertNoSchemaErrors();
        assertRead(c0(), "LOCAL_QUORUM", KEY, VALUE);
    }

    @Test
    public void t08_cassandraNodeRestartComesBack() {
        cluster.restart(c0());
        Shell.awaitNodeStates(c1(), states("UN", "UN", "UN"), SHORT);
        Shell.awaitNodeStates(c0(), states("UN", "UN", "UN"), SHORT);
        Shell.awaitCqlLogin(c0(), USER, PASS, SHORT);
        assertRead(c0(), "LOCAL_QUORUM", KEY, VALUE);
        assertNoSchemaErrors();
    }

    @Test
    public void t09_dseDownLocalQuorumInCassandraDcWorks() {
        cluster.stop(dse());
        Shell.awaitNodeStates(c0(), states("DN", "UN", "UN"), SHORT);

        write(c0(), "LOCAL_QUORUM", "dse-down", "written-without-dse");
        assertRead(c0(), "LOCAL_QUORUM", "dse-down", "written-without-dse");
        assertRead(c0(), "QUORUM", "dse-down", "written-without-dse");

        long start = System.nanoTime();
        ExecResult r = Shell.cqlRaw(c0(), USER, PASS, "ALL",
                "INSERT INTO everywhere_it.kv (k, v) VALUES ('all-without-dse', 'must-fail')");
        Duration took = Duration.ofNanos(System.nanoTime() - start);
        String out = r.getStdout() + r.getStderr();
        System.out.println("ALL write with DSE down: exit " + r.getExitCode() + " after " + took + "\n" + out);
        assertNotEquals("an ALL write with DSE down must fail", 0, r.getExitCode());
        assertTrue("expected NoHostAvailable/Unavailable, got:\n" + out,
                out.contains("NoHostAvailable") || out.contains("Unavailable"));
        assertTrue("the ALL write took " + took + " to fail", took.compareTo(Duration.ofSeconds(30)) < 0);
    }

    @Test
    public void t10_postMigrationWithoutTheJar() throws Exception {
        // 1. Remove the (stopped) DSE node from the ring.
        String dseHostId = Shell.hostId(c0(), DSE_IP);
        System.out.println("removenode path taken: " + removeNode(dseHostId));
        Shell.awaitNodeStates(c0(), states(null, "UN", "UN"), SHORT);
        Shell.awaitNodeStates(c1(), states(null, "UN", "UN"), SHORT);

        // 2. Move every EverywhereStrategy keyspace, and system_auth, to NetworkTopologyStrategy in dc2.
        List<String> everywhere = keyspacesUsing(c0(), EVERYWHERE);
        System.out.println("EverywhereStrategy keyspaces to convert: " + everywhere);
        assertTrue("expected everywhere_it and dse_system among " + everywhere,
                everywhere.containsAll(Arrays.asList("everywhere_it", "dse_system")));
        List<String> toAlter = new ArrayList<>(everywhere);
        toAlter.add("system_auth");
        for (String ks : toAlter) {
            Shell.cql(c0(), USER, PASS, "ONE", "ALTER KEYSPACE " + ks + " WITH replication = "
                    + "{'class': 'NetworkTopologyStrategy', 'dc2': 2}");
        }
        Shell.awaitSchemaAgreement(c0(), SHORT);
        assertEquals(Collections.emptyList(), keyspacesUsing(c1(), EVERYWHERE));

        // 3. Remove the jar from both nodes and restart them one at a time.
        Map<GenericContainer<?>, Integer> strategyLogsBefore = new LinkedHashMap<>();
        for (GenericContainer<?> node : cluster.cassandra()) {
            ExecResult rm = node.execInContainer("rm", MixedCluster.JAR_PATH_IN_CONTAINER);
            assertEquals("rm " + MixedCluster.JAR_PATH_IN_CONTAINER + " on " + cluster.ip(node) + ": "
                    + rm.getStderr(), 0, rm.getExitCode());
            strategyLogsBefore.put(node, count(node.getLogs(), STRATEGY_IN_USE));
        }
        for (GenericContainer<?> node : cluster.cassandra()) {
            cluster.restart(node);
            Shell.awaitNodeStates(node, states(null, "UN", "UN"), LONG);
        }

        // 4. The cluster is healthy and the data is still there, without the jar.
        Shell.awaitNodeStates(c0(), states(null, "UN", "UN"), SHORT);
        Shell.awaitNodeStates(c1(), states(null, "UN", "UN"), SHORT);
        Shell.awaitSchemaAgreement(c0(), SHORT);
        for (GenericContainer<?> node : cluster.cassandra()) {
            Shell.awaitCqlLogin(node, USER, PASS, SHORT);
            assertRead(node, "LOCAL_QUORUM", KEY, VALUE);
            String logs = node.getLogs();
            assertFalse(cluster.ip(node) + " logged '" + MISSING_STRATEGY + "'", logs.contains(MISSING_STRATEGY));
            assertEquals(cluster.ip(node) + " instantiated EverywhereStrategy after the jar was removed",
                    strategyLogsBefore.get(node).intValue(), count(logs, STRATEGY_IN_USE));
        }
    }

    /**
     * Runs {@code nodetool removenode <id>} on cassandra-0; if it has not finished after five minutes (e.g. a
     * SimpleStrategy RF1 range has no surviving source), forces it. Returns which path was taken.
     */
    private static String removeNode(String hostId) throws Exception {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<ExecResult> removal = exec.submit(() -> c0().execInContainer("nodetool", "removenode", hostId));
            try {
                ExecResult r = removal.get(5, TimeUnit.MINUTES);
                assertEquals("nodetool removenode " + hostId + ":\n" + r.getStdout() + r.getStderr(),
                        0, r.getExitCode());
                return "nodetool removenode " + hostId;
            } catch (TimeoutException e) {
                System.out.println("removenode still running after 5 minutes; status:\n"
                        + Shell.nodetool(c0(), "removenode", "status"));
                Shell.nodetool(c0(), "removenode", "force");
                ExecResult r = removal.get(2, TimeUnit.MINUTES);
                System.out.println("blocked removenode returned exit " + r.getExitCode() + ":\n" + r.getStdout()
                        + r.getStderr());
                return "nodetool removenode " + hostId + " (hung) then nodetool removenode force";
            }
        } finally {
            exec.shutdownNow();
        }
    }

    private static GenericContainer<?> dse() {
        return cluster.dse();
    }

    private static GenericContainer<?> c0() {
        return cluster.cassandra().get(0);
    }

    private static GenericContainer<?> c1() {
        return cluster.cassandra().get(1);
    }

    /** Expected {@code nodetool status} states for DSE, cassandra-0 and cassandra-1; {@code null} = not listed. */
    private static Map<String, String> states(String dse, String c0, String c1) {
        Map<String, String> m = new LinkedHashMap<>();
        if (dse != null) {
            m.put(DSE_IP, dse);
        }
        m.put(C0_IP, c0);
        m.put(C1_IP, c1);
        return m;
    }

    private static void write(GenericContainer<?> node, String cl, String key, String value) {
        Shell.cql(node, USER, PASS, cl, "INSERT INTO everywhere_it.kv (k, v) VALUES ('" + key + "', '" + value + "')");
    }

    private static void assertRead(GenericContainer<?> node, String cl, String key, String expected) {
        List<List<String>> rows = Shell.cqlRows(Shell.cql(node, USER, PASS, cl,
                "SELECT v FROM everywhere_it.kv WHERE k = '" + key + "'"));
        assertEquals("read of " + key + " at " + cl + " on " + cluster.ip(node),
                Collections.singletonList(Collections.singletonList(expected)), rows);
    }

    private static Set<String> keyspaces(GenericContainer<?> node) {
        Set<String> names = new TreeSet<>();
        for (List<String> row : Shell.cqlRows(Shell.cql(node, USER, PASS, "ONE",
                "SELECT keyspace_name FROM system_schema.keyspaces"))) {
            names.add(row.get(0));
        }
        return names;
    }

    private static void assertSameKeyspacesAsDse(GenericContainer<?> node) {
        assertEquals("keyspaces on " + cluster.ip(node) + " vs DSE", keyspaces(dse()), keyspaces(node));
    }

    private static String replication(GenericContainer<?> node, String keyspace) {
        List<List<String>> rows = Shell.cqlRows(Shell.cql(node, USER, PASS, "ONE",
                "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = '" + keyspace + "'"));
        assertEquals("replication rows for " + keyspace + " on " + cluster.ip(node), 1, rows.size());
        return rows.get(0).get(0);
    }

    private static String replicationClass(GenericContainer<?> node, String keyspace) {
        String replication = replication(node, keyspace);
        Matcher m = REPLICATION_CLASS.matcher(replication);
        assertTrue("no class in " + replication, m.find());
        return m.group(1);
    }

    private static void awaitReplication(GenericContainer<?> node, String keyspace, String strategyClass,
                                         Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        String last;
        while (!(last = replicationClass(node, keyspace)).equals(strategyClass)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(keyspace + " on " + cluster.ip(node) + " still uses " + last + " after "
                        + timeout + ", expected " + strategyClass);
            }
            sleep(2000);
        }
    }

    /** Waits until {@code nodetool getendpoints} on {@code node} lists exactly {@code expectedIps}. */
    private static void awaitEndpoints(GenericContainer<?> node, String keyspace, String table, String key,
                                       String... expectedIps) {
        Set<String> expected = new TreeSet<>(Arrays.asList(expectedIps));
        long deadline = System.nanoTime() + SHORT.toNanos();
        Set<String> last;
        while (!(last = endpoints(node, keyspace, table, key)).equals(expected)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("getendpoints " + keyspace + " " + table + " " + key + " on "
                        + cluster.ip(node) + " still " + last + " after " + SHORT + ", expected " + expected);
            }
            sleep(2000);
        }
    }

    private static Set<String> endpoints(GenericContainer<?> node, String keyspace, String table, String key) {
        String out = Shell.nodetool(node, "getendpoints", keyspace, table, key);
        Set<String> endpoints = new TreeSet<>();
        for (String line : out.split("\\r?\\n")) {
            if (!line.trim().isEmpty()) {
                endpoints.add(line.trim().replaceFirst("^/", ""));
            }
        }
        return endpoints;
    }

    private static List<String> keyspacesUsing(GenericContainer<?> node, String strategyClass) {
        List<String> names = new ArrayList<>();
        for (List<String> row : Shell.cqlRows(Shell.cql(node, USER, PASS, "ONE",
                "SELECT keyspace_name, replication FROM system_schema.keyspaces"))) {
            if (row.get(1).contains("'class': '" + strategyClass + "'")) {
                names.add(row.get(0));
            }
        }
        return names;
    }

    private static void assertNoSchemaErrors() {
        for (GenericContainer<?> node : cluster.cassandra()) {
            String logs = node.getLogs();
            assertFalse(cluster.ip(node) + " logged '" + MERGE_FAILURE + "'", logs.contains(MERGE_FAILURE));
            assertFalse(cluster.ip(node) + " logged '" + MISSING_STRATEGY + "'", logs.contains(MISSING_STRATEGY));
        }
    }

    private static int count(String haystack, String needle) {
        int n = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            n++;
        }
        return n;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting", e);
        }
    }
}
