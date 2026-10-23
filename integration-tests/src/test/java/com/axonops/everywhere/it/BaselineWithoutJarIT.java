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

import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Proves the harness reproduces the production problem: a stock Cassandra 3.11 node (no jar) cannot load the
 * schema of a DSE cluster that has EverywhereStrategy keyspaces.
 */
public class BaselineWithoutJarIT {

    private static final String MISSING_STRATEGY =
            "Unable to find replication strategy class 'org.apache.cassandra.locator.EverywhereStrategy'";

    @Test
    public void cassandraWithoutTheJarCannotLoadDseSchema() throws Exception {
        try (MixedCluster c = MixedCluster.builder()
                .subnet("172.30.1")
                .cassandraNodes(1)
                .withJar(false)
                .build()
                .start()) {
            // Record which keyspaces DSE created with EverywhereStrategy (spec open item).
            System.out.println(Shell.cql(c.dse(), "cassandra", "cassandra", "ONE",
                    "SELECT keyspace_name, replication FROM system_schema.keyspaces"));

            GenericContainer<?> node = c.startCassandraExpectingFailure(0);
            Shell.awaitLog(node, MISSING_STRATEGY, Duration.ofMinutes(5));
        }
    }
}
