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

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;

import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

/** TEMPORARY: exercises the with-jar harness paths once in CI; superseded by MixedClusterIT (Task 6). */
public class HarnessSmokeIT {

    @Test
    public void harnessJoinsRestartsAndStopsACassandraNode() {
        try (MixedCluster c = MixedCluster.builder().subnet("172.30.2").cassandraNodes(1).build().start()) {
            GenericContainer<?> node = c.joinCassandra(0);
            String ip = c.ip(node);
            Shell.awaitCassandraSchemaAgreement(node, Collections.singletonList(ip), Duration.ofMinutes(3));
            String status = Shell.nodetool(node, "status");
            System.out.println(status);
            assertTrue(status, status.contains("172.30.2.10") && status.contains(ip));
            System.out.println(Shell.nodetool(node, "describecluster"));

            c.restart(node);
            Shell.awaitCassandraSchemaAgreement(node, Collections.singletonList(ip), Duration.ofMinutes(3));

            c.stop(node);
            c.restart(node);
            Shell.awaitCassandraSchemaAgreement(node, Collections.singletonList(ip), Duration.ofMinutes(3));
            Shell.awaitCqlLogin(node, "cassandra", "cassandra", Duration.ofMinutes(2));
            System.out.println(Shell.cql(node, "cassandra", "cassandra", "ONE",
                    "SELECT keyspace_name FROM system_schema.keyspaces"));
        }
    }
}
