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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EverywhereStrategyLoadingTest {
    @BeforeClass
    public static void setUp() {
        Ring.initCassandra();
    }

    @Test
    public void cassandraResolvesTheDseClassName() throws Exception {
        Class<AbstractReplicationStrategy> cls =
            AbstractReplicationStrategy.getClass("org.apache.cassandra.locator.EverywhereStrategy");
        AbstractReplicationStrategy s = AbstractReplicationStrategy.createReplicationStrategy(
            "dse_system", cls, new TokenMetadata(), Ring.SNITCH, Collections.emptyMap());
        assertTrue(s instanceof EverywhereStrategy);
        assertTrue(s instanceof NetworkTopologyStrategy);
    }

    @Test
    public void shortNameResolvesToo() throws Exception {
        assertEquals(EverywhereStrategy.class, AbstractReplicationStrategy.getClass("EverywhereStrategy"));
    }
}
