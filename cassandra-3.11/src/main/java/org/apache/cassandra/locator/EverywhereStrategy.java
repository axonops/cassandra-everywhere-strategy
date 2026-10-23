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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.apache.cassandra.dht.Token;

/**
 * Replication strategy compatible with DataStax Enterprise's EverywhereStrategy,
 * placing a replica of every range on every node.
 */
public class EverywhereStrategy extends NetworkTopologyStrategy {
    private final TokenMetadata tokenMetadata;

    public EverywhereStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch,
                              Map<String, String> configOptions) {
        super(keyspaceName, tokenMetadata, snitch, Collections.<String, String>emptyMap());
        this.tokenMetadata = tokenMetadata;
    }

    /**
     * Every normal-token endpoint in {@code metadata}, in ring order starting at the primary owner of
     * {@code searchToken}. Uses the metadata passed in (not the field) so that pending-range
     * calculations on cloned metadata see the hypothetical ring.
     */
    @Override
    public List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata metadata) {
        Set<InetAddress> replicas = new LinkedHashSet<>();
        Iterator<Token> tokens = TokenMetadata.ringIterator(metadata.sortedTokens(), searchToken, false);
        while (tokens.hasNext()) {
            replicas.add(metadata.getEndpoint(tokens.next()));
        }
        return new ArrayList<>(replicas);
    }

    /** Number of normal-token nodes in the cluster; bootstrapping nodes are not counted. */
    @Override
    public int getReplicationFactor() {
        return dcEndpoints().size();
    }

    /** Number of normal-token nodes in {@code dc}; 0 for a null or unknown datacenter. */
    @Override
    public int getReplicationFactor(String dc) {
        if (dc == null) {
            return 0;
        }
        return dcEndpoints().get(dc).size();
    }

    /** Datacenters that currently contain at least one normal-token node. */
    @Override
    public Set<String> getDatacenters() {
        return ImmutableSet.copyOf(dcEndpoints().keySet());
    }

    /**
     * Normal-token endpoints per datacenter. Read from the cached token-map clone because
     * {@link TokenMetadata#getTopology()} asserts it is never called on StorageService's live instance,
     * which is the instance a running node passes to this strategy. The clone is invalidated on every
     * ring change, so the counts stay live.
     */
    private Multimap<String, InetAddress> dcEndpoints() {
        return tokenMetadata.cachedOnlyTokenMap().getTopology().getDatacenterEndpoints();
    }
}
