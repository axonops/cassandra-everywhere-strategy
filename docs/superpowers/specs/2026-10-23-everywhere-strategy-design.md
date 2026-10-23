# EverywhereStrategy for Apache Cassandra — Design

Date: 2026-10-23
Status: Draft for approval

## 1. Intent

DataStax Enterprise (DSE) creates several of its own keyspaces with a DSE-only
replication strategy, `org.apache.cassandra.locator.EverywhereStrategy`, which
places a replica on every node in the cluster.

When migrating from DSE 5.1 to Apache Cassandra 3.11 by attaching a new Apache
Cassandra datacenter to the existing DSE cluster, the Cassandra nodes receive
that schema, cannot load the strategy class, and fail to join. The only
workaround today is to `ALTER KEYSPACE` those keyspaces to
`NetworkTopologyStrategy` before joining. That is fragile: **DSE resets them
back to `EverywhereStrategy` whenever a DSE node restarts**, which breaks the
Cassandra datacenter.

This project ships a small jar that is dropped into the `lib/` directory of
each Apache Cassandra node. It provides a class with the exact name DSE writes
into the schema, so Cassandra nodes load the DSE schema unchanged and keep
working however often DSE rewrites it. The jar is only needed for the duration
of the migration: once the DSE nodes are gone, the keyspaces are altered to
`NetworkTopologyStrategy` and the jar is removed.

## 2. Goals

- Apache Cassandra 3.11 nodes can join and operate in a mixed DSE 5.1 / Apache
  Cassandra 3.11 cluster without any change to the DSE keyspaces.
- Replica placement on Cassandra nodes matches DSE's: every token-owning node
  in every datacenter is a replica. Reads, writes, repair and streaming behave
  consistently across both node types.
- `LOCAL_ONE`, `LOCAL_QUORUM`, `QUORUM`, `ONE`, `ALL` and `EACH_QUORUM` behave
  correctly on these keyspaces from Cassandra nodes.
- DSE restarts (which rewrite the schema) never break Cassandra nodes.
- The project layout allows adding support for other Cassandra versions (4.0,
  4.1, 5.0) later without restructuring.
- Tested against the exact production versions, DSE 5.1.31 and Apache
  Cassandra 3.11.19, with versions configurable so others can be added.

## 3. Non-goals

- Long-term production use after migration. The jar is a migration aid only.
- Supporting DSE 6.x mixed clusters (DSE 6.x and OSS Cassandra cannot share a
  ring anyway).
- Solving other DSE→Cassandra migration blockers (DSE authenticator/authorizer,
  DSE driver protocol, TDE, `dse.yaml` features). These are documented
  elsewhere.
- Publishing to Maven Central (GitHub Releases only for now).
- Implementing versions other than 3.11 in this first phase.
- Joining Cassandra nodes via normal bootstrap (`auto_bootstrap: true`). Nodes
  join with `auto_bootstrap: false` + `nodetool rebuild <dse-dc>` (see §6.0).
  Bootstrap may trip Cassandra 3.11's strict range-movement checks
  (`RangeStreamer` expects exactly one replica to leave each range); this is a
  documented limitation, not something to design around.
- DSE workloads other than plain Cassandra (Search, Analytics, Graph) and DSE
  audit/TDE features.
- Retrofitting the jar onto Cassandra nodes already in the ring using the NTS
  workaround. The jar is always installed before a node first joins.

## 3a. Target environment (from the user)

- **Exact versions: DSE 5.1.31 → Apache Cassandra 3.11.19.** Docker images
  `datastax/dse-server:5.1.31` and `cassandra:3.11.19` both exist (verified).
- Stock DSE 5.1.31 install, **Cassandra workload only** — no Search, Analytics or
  Graph. (So `solr_admin` may not exist; `dse_system` will.)
- DSE uses **internal authentication** (DseAuthenticator with the internal
  scheme), which maps onto Cassandra's `PasswordAuthenticator` /
  `CassandraAuthorizer` / `CassandraRoleManager` on the Cassandra nodes.
- New Cassandra 3.11 nodes have the jar installed **before** they first join,
  and join with `auto_bootstrap: false` then `nodetool rebuild`.

## 4. Anti-goals (would make this a failure even if it "works")

- **Copying, adapting, or depending on the Instaclustr
  `cassandra-everywhere-strategy` project.** This must be a clean-room
  implementation for both quality and political reasons. Do not read its source
  while implementing, do not link to it from the repository.
- Reflection hacks into Cassandra internals (e.g. rewriting private final fields
  of `NetworkTopologyStrategy`).
- Unsynchronised mutable shared state, or a replication factor that is wrong
  (e.g. 0) until some first call happens.
- Anything that can cause a Cassandra node to fail or refuse schema when DSE
  pushes a schema change.

## 5. Background facts (confirmed by research)

- Class name written by DSE 5.1 and 6.x: `org.apache.cassandra.locator.EverywhereStrategy`.
- DSE 5.1 keyspaces using it include `dse_system` and `solr_admin`, with no
  replication options. The full list for 5.1 must be confirmed against a live
  node (`SELECT keyspace_name, replication FROM system_schema.keyspaces`).
- DataStax has published an Apache-2.0 `EverywhereStrategy` in its open-source
  Cassandra fork (`datastax/cassandra`,
  `src/java/org/apache/cassandra/locator/EverywhereStrategy.java`), written
  against the 4.x/5.0 API. It may be used **as a behavioural reference** for DSE
  semantics. Our code is written independently; credit it in the README/NOTICE
  where behaviour is drawn from it. Its semantics:
  - replicas = all normal-token-owning ring members, starting at the token's
    primary owner in ring order;
  - bootstrapping nodes are not natural replicas, but receive pending ranges
    for the whole ring;
  - replication factor = number of ring members;
  - no replication options recognised;
  - not "partitioned" (decommission RF checks don't block).
- Cassandra 3.11 loads strategies reflectively via a public constructor
  `(String keyspaceName, TokenMetadata, IEndpointSnitch, Map<String,String>)`.
  A fully-qualified name is used as-is; a jar in `lib/` is on the classpath.
- In Cassandra 3.11, per-datacenter consistency logic (`LOCAL_QUORUM`,
  `EACH_QUORUM` in `ConsistencyLevel`, and source selection in streaming) only
  applies when the strategy is `instanceof NetworkTopologyStrategy` and uses
  `getReplicationFactor(String dc)` / `getDatacenters()`.
- Docker images exist: `datastax/dse-server:5.1.x` (up to 5.1.51) and
  `cassandra:3.11.x` (up to 3.11.19). DSE images require `DS_LICENSE=accept` and
  are licensed for non-production use; CI testing is non-production use. Pin
  tags and never redistribute images.

## 6. Decided design

### 6.0 Join procedure

The supported procedure, which the runbook, tests and design all assume:

1. Install the jar in `lib/` on the new Cassandra 3.11 node.
2. Configure the node for the new DC with `auto_bootstrap: false`.
3. Start it; it joins the ring as a normal node with no data streamed.
4. Run `nodetool rebuild <dse-dc>` to stream data.

Because rebuild does not move tokens, it bypasses the strict range-movement
path. Bootstrapping/pending-range behaviour must still be *correct* (§6.1),
because DSE-side or later operations may exercise it, but the
`auto_bootstrap: true` join is not a supported path.

### 6.1 Class

`org.apache.cassandra.locator.EverywhereStrategy extends NetworkTopologyStrategy`
(3.11 module). Extending NTS is deliberate: it is the only way for Cassandra
3.11's consistency-level and streaming code to treat the keyspace per-DC.

Behaviour:

- **Per-DC replication factor** = number of token-owning (normal) endpoints in
  that DC, computed **from the `TokenMetadata` currently in use**, not cached in
  a field. Membership changes are always reflected.
- `getReplicationFactor()` = total number of token-owning endpoints.
- `getReplicationFactor(String dc)` and `getDatacenters()` are overridden to use
  the live per-DC counts.
- `calculateNaturalEndpoints(Token, TokenMetadata)` uses **the `TokenMetadata`
  argument** (which may be a clone used for pending-range calculation), not
  `this.tokenMetadata`, and returns every normal-token-owning endpoint, with the
  token's primary owner first and no duplicates (vnodes). Ordering beyond
  "primary first, deterministic" is left to the builder.
- Bootstrapping / leaving / moving nodes: treated exactly as Cassandra 3.11
  treats them for any strategy (natural replicas = normal endpoints only;
  pending ranges computed by Cassandra's standard machinery using the
  `TokenMetadata` it passes in).
- **Options:** DSE sends none. Any options received are ignored with a single
  `WARN` log, never an exception, in both `validateOptions()` (schema pushes) and
  construction. `recognizedOptions()` returns an empty collection. Whether
  `CREATE/ALTER KEYSPACE ... EverywhereStrategy` with options issued from a
  Cassandra node is rejected or just warned is the builder's call, as long as
  schema arriving from DSE nodes never fails.
- Safe for concurrent use: no mutable shared state, or state that is safely
  published.
- Constructor passes an empty (non-null) option map to the NTS super
  constructor so NTS does not parse DC options.
- Logs at `INFO` once on construction per keyspace that it is in use, so
  operators can confirm the jar is loaded.

### 6.2 Repository layout

Maven multi-module build:

```
pom.xml                       parent: plugin versions, common config
cassandra-3.11/               the 3.11 implementation
  pom.xml                     cassandra-all 3.11.x (provided), Java 8 target
  src/main/java/org/apache/cassandra/locator/EverywhereStrategy.java
  src/test/java/...           unit tests
integration-tests/            Docker-based mixed-cluster tests (see 6.4)
docs/                         runbook, design docs
.github/workflows/            CI + release
```

- The `cassandra-all` version is a Maven property, set to **3.11.19** (the
  production version), so it can be overridden for other 3.11.x releases.
- The built jar is named
  `cassandra-everywhere-strategy-cassandra-3.11-<version>.jar`, contains only
  our class(es), no shaded dependencies.
- Future versions are added as sibling modules (`cassandra-4.0`, …). Shared
  test scenarios should be structured so they can be reused across modules
  (e.g. a shared integration test suite parameterised by image and jar).

### 6.3 Unit tests

Against Cassandra 3.11's real `TokenMetadata` and a configurable snitch,
covering at minimum:

- single DC, multi DC (including DCs of different sizes), vnodes;
- per-DC RF and total RF track membership as nodes are added/removed;
- bootstrapping nodes are not natural replicas but get pending ranges;
- leaving nodes; moving nodes;
- options present → warning, no exception;
- `calculateNaturalEndpoints` uses the passed-in `TokenMetadata`;
- reflective construction exactly as Cassandra does it (`AbstractReplicationStrategy.createReplicationStrategy`).
- consistency-level block counts (`LOCAL_QUORUM`, `EACH_QUORUM`, `QUORUM`) are
  computed correctly by Cassandra's own `ConsistencyLevel` for this strategy.

### 6.4 Integration tests (Docker)

Mixed cluster with one DC of DSE 5.1 and one DC of Apache Cassandra 3.11
(minimum one node each; two Cassandra nodes preferred so LOCAL_QUORUM is
meaningful). DSE and Cassandra image tags are parameters, defaulting to
`datastax/dse-server:5.1.31` and `cassandra:3.11.19`.

Cluster configuration *(builder's-call defaults set by the design author)*:

- DSE node: stock config, Cassandra workload only, DseAuthenticator with
  internal scheme enabled, to mirror production.
- Cassandra nodes: `PasswordAuthenticator`, `CassandraAuthorizer`,
  `CassandraRoleManager`; `auto_bootstrap: false`; joined then
  `nodetool rebuild <dse-dc>` as in §6.0.
- If an unrelated DSE→Cassandra incompatibility blocks the test cluster, it
  may be configured around in the test setup, but must be written down in the
  runbook as a known migration step.

Scenarios:

1. **Baseline failure:** Cassandra node without the jar fails to join / reports
   the missing strategy class. Proves the test reproduces the real problem.
2. Cassandra node with the jar joins (per §6.0), `nodetool rebuild` succeeds,
   and schema agreement is reached. Authenticated CQL login works on the
   Cassandra node using a role created on DSE.
3. On EverywhereStrategy keyspaces: replicas reported for a key include every
   node in both DCs (e.g. `nodetool getendpoints`).
4. Write/read at `LOCAL_QUORUM` in each DC, and at `QUORUM` / `ALL`.
5. Restart the DSE node (which rewrites the keyspaces); Cassandra nodes stay
   healthy and continue to serve these keyspaces.
6. `nodetool repair` on an EverywhereStrategy keyspace from a Cassandra node
   succeeds.
7. Post-migration path: `ALTER KEYSPACE` to NTS, remove jar, restart Cassandra
   node — it starts cleanly.

Test tooling (Testcontainers, shell + docker compose, etc.) is the builder's
choice.

### 6.5 CI / release (GitHub Actions)

- **On PR and push to main:** build, unit tests, integration tests.
- **On tag `v*`:** build, test, attach the jar(s) and SHA-256 checksums to a
  GitHub Release.
- CI matrix starts with one entry: DSE 5.1.31 × Cassandra 3.11.19. Image tags
  live in the matrix so further version pairs can be added without code changes.

### 6.6 Documentation

- `README.md`: what this is, why it exists, compatibility table, install.
- Migration runbook: install the jar on every Cassandra 3.11 node before it
  joins, join with `auto_bootstrap: false`, `nodetool rebuild <dse-dc>`,
  complete migration, decommission DSE, `ALTER KEYSPACE` the
  EverywhereStrategy keyspaces to NTS with appropriate RF, run repair, remove
  the jar, rolling restart.
- `NOTICE`: credit DataStax's Apache-2.0 implementation as behavioural
  reference.
- Licence: Apache 2.0 (already in repo).

## 7. Constraints

- Java 8 bytecode (Cassandra 3.11 runs on Java 8).
- `cassandra-all` is `provided` scope.
- No dependencies beyond what Cassandra 3.11 already ships.
- Must load via Cassandra's standard reflective mechanism; no agents, no patching.

## 8. Left to the builder

- Exact replica ordering beyond "primary owner first, deterministic, no
  duplicates".
- Integration test tooling and harness structure.
- Whether to reject options on user-issued CREATE/ALTER from a Cassandra node.
- Maven plugin choices (formatting, static analysis, etc.).

## 9. Open items for later

- Confirm the complete list of DSE 5.1 keyspaces using EverywhereStrategy on a
  live node and record it in the README (the integration test should print
  this list from the DSE container).
- Verify against the 3.11 source that `nodetool rebuild` and the
  pending-range path behave as assumed in §6.0.
