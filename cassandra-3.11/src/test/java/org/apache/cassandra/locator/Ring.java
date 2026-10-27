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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;

/** Fluent helper that builds a TokenMetadata ring for tests. */
public class Ring {
    public static final MapSnitch SNITCH = new MapSnitch();

    private static boolean initialised;

    private final TokenMetadata tm = new TokenMetadata();

    public static synchronized void initCassandra() {
        if (initialised) {
            return;
        }
        DatabaseDescriptor.clientInitialization(false);
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        DatabaseDescriptor.setEndpointSnitch(SNITCH);
        initialised = true;
    }

    public Ring() {
        initCassandra();
    }

    /**
     * The five-node "standard ring": dc1 = 127.0.0.1 (r1, 0), 127.0.0.2 (r1, 100), 127.0.0.3 (r2, 200);
     * dc2 = 127.0.1.1 (r1, 50), 127.0.1.2 (r1, 150).
     */
    public static Ring standard() {
        return new Ring()
            .node("127.0.0.1", "dc1", "r1", 0)
            .node("127.0.0.2", "dc1", "r1", 100)
            .node("127.0.0.3", "dc1", "r2", 200)
            .node("127.0.1.1", "dc2", "r1", 50)
            .node("127.0.1.2", "dc2", "r1", 150);
    }

    public Ring node(String ip, String dc, String rack, long... tokens) {
        register(ip, dc, rack);
        tm.updateNormalTokens(tokens(tokens), ip(ip));
        return this;
    }

    public Ring bootstrap(String ip, String dc, String rack, long... tokens) {
        register(ip, dc, rack);
        tm.addBootstrapTokens(tokens(tokens), ip(ip));
        return this;
    }

    public Ring leave(String ip) {
        tm.addLeavingEndpoint(ip(ip));
        return this;
    }

    public Ring move(String ip, long token) {
        tm.addMovingEndpoint(t(token), ip(ip));
        return this;
    }

    public Ring remove(String ip) {
        tm.removeEndpoint(ip(ip));
        return this;
    }

    public TokenMetadata tm() {
        return tm;
    }

    public static InetAddress ip(String s) {
        try {
            return InetAddress.getByName(s);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Token t(long v) {
        return new Murmur3Partitioner.LongToken(v);
    }

    private void register(String ip, String dc, String rack) {
        SNITCH.put(ip, dc, rack);
        tm.updateHostId(UUID.randomUUID(), ip(ip));
    }

    private static List<Token> tokens(long... values) {
        List<Token> out = new ArrayList<>();
        for (long v : values) {
            out.add(t(v));
        }
        return out;
    }
}
