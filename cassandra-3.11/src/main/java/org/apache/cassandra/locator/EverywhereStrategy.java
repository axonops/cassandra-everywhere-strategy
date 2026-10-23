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
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    @Override
    public List<InetAddress> calculateNaturalEndpoints(org.apache.cassandra.dht.Token token, TokenMetadata metadata) {
        return Collections.emptyList();
    }
}
