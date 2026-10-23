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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;

/**
 * Runs cqlsh and nodetool inside the test containers, and polls for cluster states.
 */
public final class Shell {

    private static final long POLL_MILLIS = 2000;
    private static final Pattern SCHEMA_LINE = Pattern.compile("^\\s*(\\S+):\\s*\\[(.*)\\]\\s*$");
    private static final Pattern STATUS_LINE = Pattern.compile("^([UD][NLJM])\\s+(\\d{1,3}(?:\\.\\d{1,3}){3})\\s.*$");
    private static final Pattern HOST_ID =
            Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    private static final Pattern CQL_SEPARATOR = Pattern.compile("^\\s*-+(\\+-+)*\\s*$");
    private static final Pattern CQL_ROW_COUNT = Pattern.compile("^\\s*\\(\\d+ rows?\\)\\s*$");

    private Shell() {
    }

    /** Runs cqlsh against the container's own address; throws {@link AssertionError} on a non-zero exit. */
    public static String cql(GenericContainer<?> c, String user, String pass, String consistency, String cql) {
        ExecResult r = cqlRaw(c, user, pass, consistency, cql);
        if (r.getExitCode() != 0) {
            throw new AssertionError("cqlsh failed (exit " + r.getExitCode() + ") on " + MixedCluster.ipOf(c)
                    + " running [" + cql + "] at " + consistency + "\nstdout:\n" + r.getStdout()
                    + "\nstderr:\n" + r.getStderr());
        }
        return r.getStdout();
    }

    /** Runs cqlsh against the container's own address and returns the result whatever the exit code. */
    public static ExecResult cqlRaw(GenericContainer<?> c, String user, String pass, String consistency, String cql) {
        return exec(c, "cqlsh", MixedCluster.ipOf(c), "-u", user, "-p", pass,
                "-e", "CONSISTENCY " + consistency + "; " + cql);
    }

    /** Runs nodetool in the container; throws {@link AssertionError} on a non-zero exit. */
    public static String nodetool(GenericContainer<?> c, String... args) {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "nodetool";
        System.arraycopy(args, 0, cmd, 1, args.length);
        ExecResult r = exec(c, cmd);
        if (r.getExitCode() != 0) {
            throw new AssertionError("nodetool " + String.join(" ", args) + " failed (exit " + r.getExitCode()
                    + ") on " + MixedCluster.ipOf(c) + "\nstdout:\n" + r.getStdout() + "\nstderr:\n" + r.getStderr());
        }
        return r.getStdout();
    }

    /**
     * Waits until {@code nodetool describecluster} reports exactly one schema version and no unreachable nodes.
     * Only usable once no DSE node is in the ring: a mixed DSE/Cassandra cluster always reports two versions.
     */
    public static void awaitSchemaAgreement(GenericContainer<?> c, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        String last = "";
        while (true) {
            ExecResult r = exec(c, "nodetool", "describecluster");
            last = r.getStdout() + r.getStderr();
            if (r.getExitCode() == 0) {
                Map<String, List<String>> versions = schemaVersions(r.getStdout());
                if (versions.size() == 1 && !versions.containsKey("UNREACHABLE")) {
                    return;
                }
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("No schema agreement within " + timeout + ". Last describecluster:\n" + last);
            }
            sleep();
        }
    }

    /**
     * Waits until all {@code cassandraIps} are listed under one single schema version in
     * {@code nodetool describecluster} (run on {@code anyCassandraNode}), and none of them is unreachable.
     * DSE nodes sit on their own version in a mixed cluster and are ignored.
     */
    public static void awaitCassandraSchemaAgreement(GenericContainer<?> anyCassandraNode, List<String> cassandraIps,
                                                     Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        String last = "";
        while (true) {
            ExecResult r = exec(anyCassandraNode, "nodetool", "describecluster");
            last = r.getStdout() + r.getStderr();
            if (r.getExitCode() == 0 && cassandraAgree(schemaVersions(r.getStdout()), cassandraIps)) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Cassandra nodes " + cassandraIps + " did not agree on one schema version within "
                        + timeout + ". Last describecluster:\n" + last);
            }
            sleep();
        }
    }

    /** Waits until the container's log (stdout and stderr) contains {@code text}. */
    public static void awaitLog(GenericContainer<?> c, String text, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            String logs = c.getLogs();
            if (logs.contains(text)) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Log of " + MixedCluster.ipOf(c) + " did not contain [" + text + "] within "
                        + timeout + ". Last 4000 characters:\n" + logs.substring(Math.max(0, logs.length() - 4000)));
            }
            sleep();
        }
    }

    /** Waits until a cqlsh login with the given credentials succeeds against the container. */
    public static void awaitCqlLogin(GenericContainer<?> c, String user, String pass, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            ExecResult r = cqlRaw(c, user, pass, "ONE", "SELECT release_version FROM system.local");
            if (r.getExitCode() == 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("cqlsh login as " + user + " on " + MixedCluster.ipOf(c) + " failed for "
                        + timeout + ".\nstdout:\n" + r.getStdout() + "\nstderr:\n" + r.getStderr());
            }
            sleep();
        }
    }

    /**
     * Waits until {@code nodetool status} on {@code c} lists exactly the addresses in {@code expected}, each in
     * its expected state (e.g. {@code "UN"}, {@code "DN"}).
     */
    public static void awaitNodeStates(GenericContainer<?> c, Map<String, String> expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        String last = "";
        while (true) {
            ExecResult r = exec(c, "nodetool", "status");
            last = r.getStdout() + r.getStderr();
            if (r.getExitCode() == 0 && nodeStates(r.getStdout()).equals(expected)) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("nodetool status on " + MixedCluster.ipOf(c) + " did not show " + expected
                        + " within " + timeout + ". Last status:\n" + last);
            }
            sleep();
        }
    }

    /** The host ID that {@code nodetool status} on {@code c} reports for {@code ip}. */
    public static String hostId(GenericContainer<?> c, String ip) {
        return hostId(nodetool(c, "status"), ip);
    }

    /**
     * Parses the result table of a cqlsh query into rows of trimmed column values. Lines before the header
     * (such as "Consistency level set to ...") and the "(N rows)" footer are skipped.
     */
    public static List<List<String>> cqlRows(String cqlshOutput) {
        List<List<String>> rows = new ArrayList<>();
        boolean inRows = false;
        for (String line : cqlshOutput.split("\\r?\\n")) {
            if (!inRows) {
                inRows = CQL_SEPARATOR.matcher(line).matches();
                continue;
            }
            if (CQL_ROW_COUNT.matcher(line).matches()) {
                break;
            }
            if (line.trim().isEmpty()) {
                continue;
            }
            List<String> row = new ArrayList<>();
            for (String col : line.split("\\|", -1)) {
                row.add(col.trim());
            }
            rows.add(row);
        }
        return rows;
    }

    /** Parses {@code nodetool status} into address -> state (e.g. {@code "UN"}), across all datacenters. */
    static Map<String, String> nodeStates(String status) {
        Map<String, String> states = new LinkedHashMap<>();
        for (String line : status.split("\\r?\\n")) {
            Matcher m = STATUS_LINE.matcher(line.trim());
            if (m.matches()) {
                states.put(m.group(2), m.group(1));
            }
        }
        return states;
    }

    /** The host ID on the {@code nodetool status} line for {@code ip}. */
    static String hostId(String status, String ip) {
        for (String line : status.split("\\r?\\n")) {
            Matcher m = STATUS_LINE.matcher(line.trim());
            if (m.matches() && m.group(2).equals(ip)) {
                Matcher id = HOST_ID.matcher(line);
                if (id.find()) {
                    return id.group();
                }
            }
        }
        throw new AssertionError("No host ID for " + ip + " in nodetool status:\n" + status);
    }

    /** Parses the "Schema versions:" section of {@code nodetool describecluster} into version -> addresses. */
    static Map<String, List<String>> schemaVersions(String describecluster) {
        Map<String, List<String>> versions = new LinkedHashMap<>();
        boolean inSection = false;
        for (String line : describecluster.split("\\r?\\n")) {
            if (line.trim().startsWith("Schema versions:")) {
                inSection = true;
                continue;
            }
            if (!inSection) {
                continue;
            }
            Matcher m = SCHEMA_LINE.matcher(line);
            if (m.matches()) {
                List<String> ips = new ArrayList<>();
                for (String ip : m.group(2).split(",")) {
                    if (!ip.trim().isEmpty()) {
                        ips.add(ip.trim().replaceFirst("^/", ""));
                    }
                }
                versions.put(m.group(1), ips);
            } else if (!line.trim().isEmpty()) {
                // Any later non-matching, non-blank line ends the section.
                break;
            }
        }
        return versions;
    }

    static boolean cassandraAgree(Map<String, List<String>> versions, List<String> cassandraIps) {
        List<String> unreachable = versions.get("UNREACHABLE");
        if (unreachable != null && !Collections.disjoint(unreachable, cassandraIps)) {
            return false;
        }
        for (Map.Entry<String, List<String>> e : versions.entrySet()) {
            if (!e.getKey().equals("UNREACHABLE") && e.getValue().containsAll(cassandraIps)) {
                return true;
            }
        }
        return false;
    }

    private static ExecResult exec(GenericContainer<?> c, String... cmd) {
        try {
            return c.execInContainer(cmd);
        } catch (Exception e) {
            throw new AssertionError("exec " + String.join(" ", cmd) + " failed on " + MixedCluster.ipOf(c), e);
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting", e);
        }
    }
}
