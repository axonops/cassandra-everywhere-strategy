# EverywhereStrategy for Apache Cassandra 3.11 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a drop-in jar that lets Apache Cassandra 3.11.19 nodes load and correctly serve DSE 5.1.31 keyspaces that use `org.apache.cassandra.locator.EverywhereStrategy`, during a DSE → Cassandra datacenter migration.

**Architecture:** One class, `org.apache.cassandra.locator.EverywhereStrategy`, extending Cassandra 3.11's `NetworkTopologyStrategy` so Cassandra's per-DC consistency code applies. Replication factors are derived live from the ring's normal (token-owning) endpoints; no cached or mutable state. A Maven multi-module build (one module per Cassandra version) plus a Testcontainers mixed-cluster integration suite, run in GitHub Actions.

**Tech Stack:** Java 8, Maven 3.9, `cassandra-all` 3.11.19 (provided), JUnit 4.13.2, Mockito 4.11.0, Testcontainers 1.20.4, Docker images `datastax/dse-server:5.1.31` and `cassandra:3.11.19`, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-23-everywhere-strategy-design.md`

## Global Constraints

- Class name exactly `org.apache.cassandra.locator.EverywhereStrategy`; public constructor `(String, TokenMetadata, IEndpointSnitch, Map<String,String>)`.
- Java 8 bytecode (`maven.compiler.source/target = 1.8`); build and CI on JDK 8 (Temurin).
- `cassandra-all` version property `cassandra.version` = `3.11.19`, scope `provided`. No runtime dependencies beyond what Cassandra 3.11 ships; the jar contains only our classes.
- Jar file name: `cassandra-everywhere-strategy-cassandra-3.11-<version>.jar`. groupId `com.axonops`.
- Integration image defaults: `datastax/dse-server:5.1.31` (env `DS_LICENSE=accept`), `cassandra:3.11.19`; overridable via `-Ddse.image=` / `-Dcassandra.image=`.
- Clean room: do not read, copy, adapt, depend on or link to the Instaclustr `cassandra-everywhere-strategy` project. DataStax's Apache-2.0 fork may be consulted for behaviour only.
- No reflection into Cassandra internals in production code. No mutable shared state.
- Options passed to the strategy are ignored with a WARN, never an exception.
- Supported join path: `auto_bootstrap: false` then `nodetool rebuild <dse-dc>`.
- Licence Apache 2.0 (already in repo). New source files carry the Apache 2.0 header.
- Local containers: single `podman` / `podman-compose` commands work (one plain command per Bash call, no `;`/`&&`/pipes), but the Docker API socket is unreachable from the sandbox, so the Testcontainers suite cannot run locally. Use podman by hand for quick checks (image config paths, auth setup, join steps) before pushing; the Testcontainers suite is verified in GitHub Actions (repo `axonops/cassandra-everywhere-strategy`, private), driven with `gh`. All work happens on a feature branch, never directly on `main`.

## Review Focus

1. **Empty ring at startup** (schema is loaded from disk before gossip populates `TokenMetadata`): `getReplicationFactor()` = 0, `getReplicationFactor(dc)` = 0, `getDatacenters()` empty, `calculateNaturalEndpoints` returns an empty list — no `AssertionError` (NTS's version asserts here). → test in Task 2.
2. **Unknown or null DC name** passed to `getReplicationFactor(String)` returns 0, no NPE (Cassandra calls it with `DatabaseDescriptor.getLocalDataCenter()`, which can be null in some modes). → test in Task 2.
3. **Unexpected options arriving from a DSE schema push**, e.g. `{'replication_factor':'3'}` or `{'dc1':'abc'}`, which would make NTS's constructor throw: strategy constructs, logs a WARN. → test in Task 3.
4. **Cassandra node restart** while the DSE schema is present: node restarts and returns to `UN`. → scenario in Task 6.
5. **Whole DSE DC down**: `LOCAL_QUORUM` in the Cassandra DC still succeeds; `ALL` fails with an unavailable error rather than hanging. → scenario in Task 6.

---

## File Structure

```
pom.xml                                                     parent POM (versions, plugin mgmt, modules)
.gitignore
cassandra-3.11/pom.xml                                      artifactId cassandra-everywhere-strategy-cassandra-3.11
cassandra-3.11/src/main/java/org/apache/cassandra/locator/EverywhereStrategy.java
cassandra-3.11/src/test/java/org/apache/cassandra/locator/MapSnitch.java              test snitch: ip -> (dc, rack)
cassandra-3.11/src/test/java/org/apache/cassandra/locator/Ring.java                   test fixture building TokenMetadata
cassandra-3.11/src/test/java/org/apache/cassandra/locator/EverywhereStrategyLoadingTest.java
cassandra-3.11/src/test/java/org/apache/cassandra/locator/EverywhereStrategyPlacementTest.java
cassandra-3.11/src/test/java/org/apache/cassandra/locator/EverywhereStrategyOptionsTest.java
cassandra-3.11/src/test/java/org/apache/cassandra/locator/EverywhereStrategyConsistencyTest.java
integration-tests/pom.xml                                   failsafe, profile `integration`
integration-tests/src/test/java/com/axonops/everywhere/it/MixedCluster.java           Testcontainers cluster builder
integration-tests/src/test/java/com/axonops/everywhere/it/Shell.java                  cqlsh/nodetool exec helpers
integration-tests/src/test/java/com/axonops/everywhere/it/BaselineWithoutJarIT.java
integration-tests/src/test/java/com/axonops/everywhere/it/MixedClusterIT.java
integration-tests/src/test/resources/docker/dse/Dockerfile
integration-tests/src/test/resources/docker/cassandra/Dockerfile
.github/workflows/ci.yml
.github/workflows/release.yml
README.md
docs/migration-runbook.md
NOTICE
```

Tests live in package `org.apache.cassandra.locator` so they can use package-private Cassandra test hooks if needed; the shared integration code lives in a separate module so a future `cassandra-4.x` module reuses it by parameter (image + jar path).

---

### Task 1: Project bootstrap and reflectively-loadable strategy skeleton

**Files:**
- Create: `pom.xml`, `.gitignore`, `cassandra-3.11/pom.xml`
- Create: `cassandra-3.11/src/main/java/org/apache/cassandra/locator/EverywhereStrategy.java`
- Create: `cassandra-3.11/src/test/java/org/apache/cassandra/locator/MapSnitch.java`, `Ring.java`
- Test: `cassandra-3.11/src/test/java/org/apache/cassandra/locator/EverywhereStrategyLoadingTest.java`

**Interfaces:**
- Produces:
  - `public class EverywhereStrategy extends NetworkTopologyStrategy` with `public EverywhereStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String,String> configOptions)`. Passes `Collections.emptyMap()` to `super`; keeps its own `private final TokenMetadata tokenMetadata` (the superclass field is private).
  - `MapSnitch extends AbstractNetworkTopologySnitch`: `MapSnitch put(String ip, String dc, String rack)`; `getDatacenter`/`getRack` look up the map; unknown address → `"unknown-dc"`/`"unknown-rack"`.
  - `Ring`: `static void initCassandra()` (idempotent: `DatabaseDescriptor.clientInitialization(false)`, `setPartitionerUnsafe(Murmur3Partitioner.instance)`, `setEndpointSnitch(SNITCH)`); `static final MapSnitch SNITCH`; `Ring()` wraps a fresh `TokenMetadata`; `Ring node(String ip, String dc, String rack, long... tokens)` (registers in snitch, `updateNormalTokens`); `Ring bootstrap(String ip, String dc, String rack, long... tokens)` (`addBootstrapTokens`); `Ring leave(String ip)`; `Ring move(String ip, long token)`; `Ring remove(String ip)`; `TokenMetadata tm()`; `static InetAddress ip(String)`; `static Token t(long)` (`new Murmur3Partitioner.LongToken(v)`). Every node gets a random host id via `tm.updateHostId` so `getAllEndpoints()` sees it.

- [ ] **Step 1: Write the build files**

Parent `pom.xml`: groupId `com.axonops`, artifactId `cassandra-everywhere-strategy-parent`, version `0.1.0-SNAPSHOT`, packaging `pom`, modules `cassandra-3.11` and `integration-tests` (add the latter in Task 5). Properties: `cassandra.version=3.11.19`, `junit.version=4.13.2`, `mockito.version=4.11.0`, `testcontainers.version=1.20.4`, `maven.compiler.source/target=1.8`, `project.build.sourceEncoding=UTF-8`. Pin `maven-compiler-plugin` 3.13.0, `maven-surefire-plugin` 3.5.2, `maven-failsafe-plugin` 3.5.2, `maven-jar-plugin` 3.4.2, `maven-dependency-plugin` 3.8.1.

`cassandra-3.11/pom.xml`: artifactId `cassandra-everywhere-strategy-cassandra-3.11`; `cassandra-all` `${cassandra.version}` scope `provided`; junit, mockito-core test scope; `<finalName>${project.artifactId}-${project.version}</finalName>` (gives the required jar name). Surefire `argLine` `-ea`.

`.gitignore`: `target/`, `.idea/`, `*.iml`, `.vscode/`. (`cassandra-everywhere-strategy.iml` is staged in git; leave it staged — do not delete user files; just ignore future ones.)

- [ ] **Step 2: Write the failing test**

```java
public class EverywhereStrategyLoadingTest {
    @BeforeClass public static void setUp() { Ring.initCassandra(); }

    @Test public void cassandraResolvesTheDseClassName() throws Exception {
        Class<AbstractReplicationStrategy> cls =
            AbstractReplicationStrategy.getClass("org.apache.cassandra.locator.EverywhereStrategy");
        AbstractReplicationStrategy s = AbstractReplicationStrategy.createReplicationStrategy(
            "dse_system", cls, new TokenMetadata(), Ring.SNITCH, Collections.emptyMap());
        assertTrue(s instanceof EverywhereStrategy);
        assertTrue(s instanceof NetworkTopologyStrategy);
    }

    @Test public void shortNameResolvesToo() throws Exception {
        assertEquals(EverywhereStrategy.class, AbstractReplicationStrategy.getClass("EverywhereStrategy"));
    }
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `mvn -B -pl cassandra-3.11 test -Dtest=EverywhereStrategyLoadingTest`
Expected: compilation failure, `cannot find symbol: class EverywhereStrategy`.

- [ ] **Step 4: Implement the skeleton**

Constructor only, plus stub overrides needed to compile (`calculateNaturalEndpoints` returning `Collections.emptyList()` is acceptable until Task 2). Add `MapSnitch` and `Ring`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn -B verify`
Expected: `BUILD SUCCESS`, 2 tests run, jar `cassandra-3.11/target/cassandra-everywhere-strategy-cassandra-3.11-0.1.0-SNAPSHOT.jar` exists and `unzip -l` lists only `org/apache/cassandra/locator/EverywhereStrategy*.class` plus `META-INF/`.

- [ ] **Step 6: Commit**

```bash
git add pom.xml .gitignore cassandra-3.11
git commit -m "build: bootstrap Maven project with loadable EverywhereStrategy skeleton"
```

---

### Task 2: Replica placement and live replication factors

**Files:**
- Modify: `cassandra-3.11/src/main/java/org/apache/cassandra/locator/EverywhereStrategy.java`
- Test: `cassandra-3.11/src/test/java/org/apache/cassandra/locator/EverywhereStrategyPlacementTest.java`

**Interfaces:**
- Consumes: `Ring`, `MapSnitch` (Task 1).
- Produces (all overrides on `EverywhereStrategy`):
  - `List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata metadata)` — uses the **argument**. Walk `TokenMetadata.ringIterator(metadata.sortedTokens(), searchToken, false)`, add `metadata.getEndpoint(token)` to a `LinkedHashSet`, return as `ArrayList`. Empty ring → empty list.
  - `int getReplicationFactor()` — `tokenMetadata.getTopology().getDatacenterEndpoints().size()` on the field (normal endpoints only; topology excludes bootstrapping nodes).
  - `int getReplicationFactor(String dc)` — `dc == null ? 0 : getDatacenterEndpoints().get(dc).size()`.
  - `Set<String> getDatacenters()` — `ImmutableSet.copyOf(getDatacenterEndpoints().keySet())`.

Fixture used by most tests ("standard ring"):

| ip | dc | rack | token |
|---|---|---|---|
| 127.0.0.1 | dc1 | r1 | 0 |
| 127.0.0.2 | dc1 | r1 | 100 |
| 127.0.0.3 | dc1 | r2 | 200 |
| 127.0.1.1 | dc2 | r1 | 50 |
| 127.0.1.2 | dc2 | r1 | 150 |

- [ ] **Step 1: Write the failing tests**

```java
@Test public void everyNormalNodeIsAReplicaInRingOrderFromThePrimaryOwner() {
    // token 60 -> first token >= 60 is 100 (127.0.0.2)
    assertEquals(ips("127.0.0.2","127.0.1.2","127.0.0.3","127.0.0.1","127.0.1.1"),
                 strategy(ring).calculateNaturalEndpoints(t(60), ring.tm()));
}
@Test public void vnodesDoNotProduceDuplicates()          // 127.0.0.1 also owns 300 -> still 5 distinct, primary for t(250) is 127.0.0.1
@Test public void replicationFactorsArePerDcNodeCounts()   // rf(dc1)=3, rf(dc2)=2, rf()=5, getDatacenters()={dc1,dc2}
@Test public void replicationFactorsTrackMembership()      // node 127.0.1.3/dc2 added -> rf(dc2)=3, rf()=6; remove 127.0.1.1 -> rf(dc2)=2, rf()=5 (same strategy instance)
@Test public void cachedNaturalEndpointsSeeANewNode()      // getNaturalEndpoints(t(60)) size 5; add node; size 6
@Test public void bootstrappingNodeIsNotANaturalReplica()  // bootstrap 127.0.1.3/dc2 token 175: not in calculateNaturalEndpoints, rf(dc2)==2
@Test public void bootstrappingNodeGetsTheWholeRingAsPending()
    // tm.calculatePendingRanges(s, "ks"); tm.getPendingRanges("ks", ip("127.0.1.3")).size() == 6
@Test public void leavingNodeCreatesNoPendingRanges()      // leave 127.0.0.3; calculatePendingRanges; getPendingRanges("ks", x) empty for all 5 ips
@Test public void movingNodeCreatesNoPendingRanges()       // move 127.0.0.2 -> 120; same assertion
@Test public void usesTheTokenMetadataItIsGiven()          // strategy on standard ring; calculateNaturalEndpoints(t(0), otherRingWith2Nodes.tm()).size()==2
@Test public void emptyRingIsSafe()                        // new TokenMetadata(): rf()=0, rf("dc1")=0, getDatacenters() empty, calculateNaturalEndpoints(t(0), tm) empty
@Test public void unknownOrNullDcHasReplicationFactorZero() // rf("nope")==0, rf(null)==0
```

`strategy(ring)` = `new EverywhereStrategy("ks", ring.tm(), Ring.SNITCH, Collections.emptyMap())`; `ips(...)` maps to `List<InetAddress>`.

- [ ] **Step 2: Run them to verify they fail**

Run: `mvn -B -pl cassandra-3.11 test -Dtest=EverywhereStrategyPlacementTest`
Expected: FAIL — placement tests get empty lists, RF tests get 0.

- [ ] **Step 3: Implement the four overrides** as described in Interfaces.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -B -pl cassandra-3.11 test`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add cassandra-3.11
git commit -m "feat: place replicas on every normal node with live per-DC replication factors"
```

---

### Task 3: Option handling, logging and NTS hooks that must not run

**Files:**
- Modify: `cassandra-3.11/src/main/java/org/apache/cassandra/locator/EverywhereStrategy.java`
- Test: `cassandra-3.11/src/test/java/org/apache/cassandra/locator/EverywhereStrategyOptionsTest.java`

**Interfaces:**
- Produces:
  - Constructor logs `INFO "EverywhereStrategy in use for keyspace {}"` and, when `configOptions` is non-null and non-empty, one `WARN "EverywhereStrategy ignores replication options for keyspace {}: {}"`.
  - `void validateOptions()` — no-op (never throws).
  - `void maybeWarnOnOptions()` — no-op (NTS's version touches `StorageService`).
  - `Collection<String> recognizedOptions()` — `Collections.emptySet()`.
  - Decision for spec §8: user-issued `CREATE/ALTER` with options on a Cassandra node is **warned, not rejected** (same path; simplest and never fails a DSE push).

- [ ] **Step 1: Write the failing tests**

Capture logs with a logback `ListAppender<ILoggingEvent>` attached to logger `org.apache.cassandra.locator.EverywhereStrategy` (logback is on the `cassandra-all` classpath).

```java
@Test public void dseSchemaPushWithReplicationFactorOptionIsAccepted()   // createReplicationStrategy(..., {"replication_factor":"3"}) returns EverywhereStrategy
@Test public void dseSchemaPushWithNonNumericDcOptionIsAccepted()        // {"dc1":"abc"} accepted
@Test public void userCreateOrAlterWithOptionsIsAccepted()               // AbstractReplicationStrategy.validateReplicationStrategy("ks", cls, tm, SNITCH, {"dc1":"3"}) does not throw
@Test public void optionsProduceExactlyOneWarning()                      // one WARN event containing "ignores replication options" and "dc1"
@Test public void noOptionsProduceNoWarningButOneInfo()                  // 0 WARN, 1 INFO containing "EverywhereStrategy in use for keyspace ks"
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -B -pl cassandra-3.11 test -Dtest=EverywhereStrategyOptionsTest`
Expected: FAIL — `validateReplicationStrategy` path errors in NTS `maybeWarnOnOptions` (`StorageService` init) and/or no log events.

- [ ] **Step 3: Implement** the constructor logging and the three overrides.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -B -pl cassandra-3.11 test`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add cassandra-3.11
git commit -m "feat: ignore replication options with a warning and log strategy use"
```

---

### Task 4: Consistency-level block counts via Cassandra's own code

**Files:**
- Test: `cassandra-3.11/src/test/java/org/apache/cassandra/locator/EverywhereStrategyConsistencyTest.java`
- Modify: `cassandra-3.11/pom.xml` (only if Mockito needs `mockito-inline` to mock `Keyspace`; prefer plain `mockito-core`)

**Interfaces:**
- Consumes: `EverywhereStrategy` overrides from Task 2, `Ring`.

`LOCAL_QUORUM` itself needs `DatabaseDescriptor.getLocalDataCenter()`, which is only set by daemon initialisation; it is covered by the integration test (Task 6). `EACH_QUORUM` exercises the same `localQuorumFor(dc)` NTS-only path.

- [ ] **Step 1: Write the tests** (standard ring from Task 2; `Keyspace ks = mock(Keyspace.class); when(ks.getReplicationStrategy()).thenReturn(strategy)`)

```java
@Test public void eachQuorumSumsPerDcQuorums() { assertEquals(4, ConsistencyLevel.EACH_QUORUM.blockFor(ks)); } // (3/2+1)+(2/2+1)
@Test public void quorumUsesTotalNodeCount()   { assertEquals(3, ConsistencyLevel.QUORUM.blockFor(ks)); }      // 5/2+1
@Test public void allUsesTotalNodeCount()      { assertEquals(5, ConsistencyLevel.ALL.blockFor(ks)); }
@Test public void blockForTracksMembership()   // add 127.0.1.3/dc2 -> EACH_QUORUM 4 (2+2), ALL 6
```

- [ ] **Step 2: Run them**

Run: `mvn -B -pl cassandra-3.11 test -Dtest=EverywhereStrategyConsistencyTest`
Expected: PASS (behaviour already implemented in Task 2; this task pins it). If any fail, fix `EverywhereStrategy`, not the test.

- [ ] **Step 3: Commit**

```bash
git add cassandra-3.11
git commit -m "test: pin consistency-level block counts for EverywhereStrategy"
```

---

### Task 5: Integration harness, baseline-failure test, and CI

**Files:**
- Modify: `pom.xml` (add module `integration-tests`)
- Create: `integration-tests/pom.xml`
- Create: `integration-tests/src/test/resources/docker/dse/Dockerfile`, `integration-tests/src/test/resources/docker/cassandra/Dockerfile`
- Create: `integration-tests/src/test/java/com/axonops/everywhere/it/MixedCluster.java`, `Shell.java`
- Test: `integration-tests/src/test/java/com/axonops/everywhere/it/BaselineWithoutJarIT.java`
- Create: `.github/workflows/ci.yml`, `.github/workflows/release.yml`

**Interfaces:**
- Consumes: the jar from Task 1's module (copied by `maven-dependency-plugin:copy` in `pre-integration-test` to `integration-tests/target/plugin/`; path passed to tests as system property `everywhere.jar`).
- Produces:
  - `MixedCluster` (implements `AutoCloseable`): `static MixedCluster.Builder builder()`; builder `.dseImage(String)` (default `System.getProperty("dse.image","datastax/dse-server:5.1.31")`), `.cassandraImage(String)` (default from `cassandra.image`, `cassandra:3.11.19`), `.cassandraNodes(int)`, `.withJar(boolean)`, `MixedCluster build()`; `MixedCluster start()` starts the DSE node only and returns `this`; `GenericContainer<?> dse()`; `GenericContainer<?> joinCassandra(int index)` starts Cassandra node `index` (0-based) and waits for CQL; `GenericContainer<?> startCassandraExpectingFailure(int index)` starts it without waiting for CQL; `List<GenericContainer<?>> cassandra()` (same order as `index`); `String ip(GenericContainer<?>)`; `void restart(GenericContainer<?>)`, `void stop(GenericContainer<?>)` (same container, fixed IP kept).
  - Fixed addressing on a dedicated Docker network with subnet `172.30.0.0/24`: DSE `172.30.0.10` (DC `dc1`, rack `r1`), Cassandra nodes `172.30.0.20`, `172.30.0.21` (DC `dc2`, rack `r1`). Fixed IPs are required because tests restart containers.
  - Cluster name `everywhere-it`; both node types use `GossipingPropertyFileSnitch`; seeds = `172.30.0.10`; heap `MAX_HEAP_SIZE=1G`, `HEAP_NEWSIZE=256M`.
  - `Shell`: `static String cql(GenericContainer<?> c, String user, String pass, String consistency, String cql)` (runs `cqlsh <ip> -u .. -p .. -e "CONSISTENCY <cl>; <cql>"`, throws `AssertionError` with stdout+stderr on non-zero exit); `static ExecResult cqlRaw(...)` same but returns result; `static String nodetool(GenericContainer<?> c, String... args)` (throws on non-zero); `static void awaitSchemaAgreement(GenericContainer<?> c, Duration timeout)` (polls `nodetool describecluster` until one schema version); `static void awaitLog(GenericContainer<?> c, String text, Duration timeout)` (polls `c.getLogs()`).
- Docker images built with Testcontainers `ImageFromDockerfile` from the resources dirs:
  - DSE: `FROM ${dse.image}`; set in the DSE `cassandra.yaml`: `authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator`, `authorizer: com.datastax.bdp.cassandra.auth.DseAuthorizer`, `role_manager: com.datastax.bdp.cassandra.auth.DseRoleManager`, `endpoint_snitch: GossipingPropertyFileSnitch`; in `dse.yaml`: `authentication_options.enabled: true`, `default_scheme: internal`, `authorization_options.enabled: true`. Verify config paths inside the 5.1.31 image (expected under `/opt/dse/resources/cassandra/conf/` and `/opt/dse/resources/dse/conf/`) and how its entrypoint applies `DC`/`RACK`/`SEEDS`/`CLUSTER_NAME` env vars.
  - Cassandra: `FROM ${cassandra.image}`; in `/etc/cassandra/cassandra.yaml`: `authenticator: PasswordAuthenticator`, `authorizer: CassandraAuthorizer`, `role_manager: CassandraRoleManager`; when `withJar`, `COPY` the jar to `/opt/cassandra/lib/`. Start with `JVM_EXTRA_OPTS=-Dcassandra.auto_bootstrap=false`, `CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch`, `CASSANDRA_DC=dc2`, `CASSANDRA_RACK=r1`, `CASSANDRA_SEEDS=172.30.0.10`, `CASSANDRA_CLUSTER_NAME=everywhere-it`.
  - If an unrelated DSE→Cassandra incompatibility blocks the cluster, configure around it here **and** record it for `docs/migration-runbook.md` (Task 7).
- `integration-tests/pom.xml`: depends on `cassandra-everywhere-strategy-cassandra-3.11` (for build order), `org.testcontainers:testcontainers`, junit. Failsafe bound only in profile `integration` (`mvn verify` without the profile skips ITs); failsafe passes `everywhere.jar`, `dse.image`, `cassandra.image`; Failsafe `forkedProcessTimeoutInSeconds` 3600. Container logs are written to `integration-tests/target/container-logs/<name>.log` via `Slf4jLogConsumer`/`ToStringConsumer` dumped on close.

- [ ] **Step 0: Check the images by hand with podman**

Before writing the harness, use single `podman` commands to confirm the facts the Dockerfiles depend on. Write findings into the Dockerfiles as comments:
- `podman run --rm --entrypoint cat docker.io/datastax/dse-server:5.1.31 <path>`: where the DSE 5.1.31 `cassandra.yaml` and `dse.yaml` live and their default authenticator and snitch.
- How the DSE entrypoint applies `DC`, `RACK`, `SEEDS` and `CLUSTER_NAME`.
- That `/etc/cassandra/cassandra.yaml` and `/opt/cassandra/lib/` exist in `docker.io/library/cassandra:3.11.19`.

Optionally, bring up DSE plus one Cassandra node with a scratch compose file in `$TMPDIR` (`podman-compose -f … up -d`, then `podman exec … nodetool status`) to check the join steps before the first CI run.

- [ ] **Step 1: Write the baseline test**

```java
public class BaselineWithoutJarIT {
    @Test public void cassandraWithoutTheJarCannotLoadDseSchema() throws Exception {
        try (MixedCluster c = MixedCluster.builder().cassandraNodes(1).withJar(false).build().start()) {
            // print the EverywhereStrategy keyspaces DSE created (spec open item)
            System.out.println(Shell.cql(c.dse(), "cassandra", "cassandra", "ONE",
                "SELECT keyspace_name, replication FROM system_schema.keyspaces"));
            GenericContainer<?> node = c.startCassandraExpectingFailure(0); // starts without waiting for CQL
            // within 5 minutes the log must contain:
            //   Unable to find replication strategy class 'org.apache.cassandra.locator.EverywhereStrategy'
            Shell.awaitLog(node, "Unable to find replication strategy class 'org.apache.cassandra.locator.EverywhereStrategy'", Duration.ofMinutes(5));
        }
    }
}
```

- [ ] **Step 2: Write `ci.yml`**

- Triggers: `pull_request`, `push` to `main`.
- Job `build`: `ubuntu-latest`, `actions/checkout@v4`, `actions/setup-java@v4` (`distribution: temurin`, `java-version: '8'`, `cache: maven`), `mvn -B verify`; upload `**/target/surefire-reports` on failure (`actions/upload-artifact@v4`).
- Job `integration`: `needs: build`; matrix `include: [{dse: '5.1.31', cassandra: '3.11.19'}]`; `mvn -B verify -Pintegration -Ddse.image=datastax/dse-server:${{ matrix.dse }} -Dcassandra.image=cassandra:${{ matrix.cassandra }}`; `timeout-minutes: 60`; upload `integration-tests/target/failsafe-reports` and `integration-tests/target/container-logs` with `if: always()`.
- `permissions: contents: read`.

- [ ] **Step 3: Write `release.yml`**

- Trigger: `push` tags `v*`. `permissions: contents: write`.
- Steps: checkout, setup-java (as above), `mvn -B versions:set -DnewVersion=${GITHUB_REF_NAME#v} -DgenerateBackupPoms=false`, `mvn -B verify -Pintegration`, `sha256sum` each `cassandra-*/target/cassandra-everywhere-strategy-*.jar` into `<jar>.sha256`, publish with `softprops/action-gh-release@v2` (`files:` the jars and `.sha256` files, `generate_release_notes: true`).

- [ ] **Step 4: Verify locally what can be verified**

Run: `mvn -B verify` → `BUILD SUCCESS`, ITs skipped. No container runtime is available locally, so integration tests are verified in GitHub Actions: commit, `git push -u origin <branch>`, open a draft PR (`gh pr create --draft`), then `gh run watch` and, on failure, `gh run view --log-failed` and `gh run download` for the container-log artifacts.
Expected: `build` and `integration` jobs green; `BaselineWithoutJarIT` PASS (proves the harness reproduces the production failure).

- [ ] **Step 5: Commit**

```bash
git add pom.xml integration-tests .github
git commit -m "test: add mixed DSE/Cassandra integration harness, baseline test and CI workflows"
```

---

### Task 6: Mixed-cluster integration scenarios

**Files:**
- Test: `integration-tests/src/test/java/com/axonops/everywhere/it/MixedClusterIT.java`

**Interfaces:**
- Consumes: `MixedCluster`, `Shell` (Task 5).

One cluster shared by all methods (`@BeforeClass` start, `@AfterClass` close), methods ordered with `@FixMethodOrder(MethodSorters.NAME_ASCENDING)`; each step depends on the previous. Credentials: default superuser `cassandra`/`cassandra` (test-only).

`@BeforeClass` setup, on DSE before any Cassandra node joins:
1. `CREATE ROLE migrator WITH PASSWORD = 'migrator' AND LOGIN = true AND SUPERUSER = true`.
2. `CREATE KEYSPACE everywhere_it WITH replication = {'class': 'org.apache.cassandra.locator.EverywhereStrategy'}` and `CREATE TABLE everywhere_it.kv (k text PRIMARY KEY, v text)`. If DSE 5.1.31 refuses EverywhereStrategy for a user keyspace, use a test table in `dse_system` instead and note it in the test.
3. Then on each Cassandra node in turn: `joinCassandra(i)`; after both have joined, `ALTER KEYSPACE system_auth WITH replication = {'class':'NetworkTopologyStrategy','dc1':1,'dc2':2}` (run on DSE).

- [ ] **Step 1: Write the scenarios**

```java
@Test public void t01_cassandraNodesJoinAndAgreeOnSchema()   // nodetool status on DSE shows 3 UN; awaitSchemaAgreement(cassandra(0), 2 min); Cassandra log contains "EverywhereStrategy in use for keyspace dse_system"
@Test public void t02_rebuildFromDseDcSucceeds()             // nodetool rebuild -- dc1 on each Cassandra node exits 0
@Test public void t03_roleCreatedOnDseLogsInOnCassandra()    // Shell.cql(cassandra(0), "migrator","migrator","ONE","SELECT now() FROM system.local") succeeds
@Test public void t04_everyNodeIsAReplica()                  // nodetool getendpoints everywhere_it kv somekey on cassandra(0) lists exactly 172.30.0.10, .20, .21
@Test public void t05_readsAndWritesAtEveryConsistencyLevel()
    // write at LOCAL_QUORUM via DSE, read at LOCAL_QUORUM via cassandra(0) -> value; write LOCAL_QUORUM via cassandra(1), read LOCAL_QUORUM via DSE;
    // write+read at QUORUM, EACH_QUORUM, ALL via cassandra(0)
@Test public void t06_repairFromCassandraNodeSucceeds()      // nodetool repair -full everywhere_it on cassandra(0) exits 0
@Test public void t07_dseRestartDoesNotBreakCassandra()
    // on DSE: ALTER KEYSPACE dse_system WITH replication = {'class':'NetworkTopologyStrategy','dc1':1,'dc2':2}; restart DSE container;
    // wait for DSE CQL; awaitSchemaAgreement; system_schema.keyspaces on cassandra(0) shows dse_system class org.apache.cassandra.locator.EverywhereStrategy;
    // all 3 nodes UN; t05 LOCAL_QUORUM read on cassandra(0) still returns the value
@Test public void t08_cassandraNodeRestartComesBack()        // restart cassandra(0) container; returns UN; LOCAL_QUORUM read on it succeeds
@Test public void t09_dseDownLocalQuorumInCassandraDcWorks()
    // stop DSE container; on cassandra(0): LOCAL_QUORUM write+read succeed; ALL write fails with output containing "Unavailable" / "NoHostAvailable" (cqlRaw non-zero exit) within 30 s
@Test public void t10_postMigrationWithoutTheJar()
    // nodetool removenode <dse host id> from cassandra(0); ALTER every keyspace whose class is EverywhereStrategy (query system_schema.keyspaces) to
    // {'class':'NetworkTopologyStrategy','dc2':2}; ALTER system_auth likewise; rm /opt/cassandra/lib/cassandra-everywhere-strategy-*.jar on both nodes;
    // restart both; both UN; schema agreement; LOCAL_QUORUM read of everywhere_it.kv returns the value
```

Container restart must keep the fixed IP (use `dockerClient.restartContainerCmd` / `stopContainerCmd` + `startContainerCmd` on the same container, not a new container).

- [ ] **Step 2: Run them**

Run: push the branch and watch the CI `integration` job (`gh run watch`; `gh run view --log-failed`; `gh run download` for container logs). Locally with Docker the equivalent is `mvn -B -Pintegration verify -Dit.test=MixedClusterIT`.
Expected: 10 tests PASS. On failure, inspect `integration-tests/target/container-logs/`. A failure caused by `EverywhereStrategy` is fixed in the strategy (with a new unit test in Tasks 2–3 reproducing it); a failure caused by an unrelated DSE incompatibility is configured around in `MixedCluster` and recorded for the runbook.

- [ ] **Step 3: Commit**

```bash
git add integration-tests
git commit -m "test: cover join, rebuild, consistency, repair, restarts and post-migration in mixed cluster"
```

---

### Task 7: Documentation

**Files:**
- Create: `README.md`, `docs/migration-runbook.md`, `NOTICE`

- [ ] **Step 1: Write `NOTICE`**

Project name and copyright AxonOps; states that behaviour of DataStax's Apache-2.0 `EverywhereStrategy` (github.com/datastax/cassandra) was used as a behavioural reference, no code copied.

- [ ] **Step 2: Write `README.md`**

What it is and why (the restart-reset problem in two short paragraphs), compatibility table (Cassandra 3.11.19 ↔ DSE 5.1.31 tested), install (copy jar to `lib/` of every Cassandra node before it first joins), behaviour summary (replicas on every token-owning node; per-DC RF = DC node count; options ignored with WARN; INFO log line to confirm loading), build (`mvn verify`, `mvn -Pintegration verify` needs Docker), the list of DSE 5.1.31 EverywhereStrategy keyspaces as printed by `BaselineWithoutJarIT`, limitation: `auto_bootstrap: true` join is not supported. No link to or mention of other third-party implementations.

- [ ] **Step 3: Write `docs/migration-runbook.md`**

Ordered steps: prerequisites (matching cluster name, `GossipingPropertyFileSnitch`, Cassandra-side auth classes `PasswordAuthenticator`/`CassandraAuthorizer`/`CassandraRoleManager`); install jar; `auto_bootstrap: false`; start nodes one at a time; `ALTER KEYSPACE system_auth` (and application keyspaces) to include the new DC; `nodetool rebuild -- <dse-dc>` on each node; verify (INFO log line, `nodetool describecluster`, `nodetool getendpoints`); cut clients over; decommission DSE nodes; `ALTER` each EverywhereStrategy keyspace to `NetworkTopologyStrategy` with an explicit RF; `nodetool repair -full` on those keyspaces; remove the jar; rolling restart. Include any workarounds recorded in Tasks 5–6. Note that a node is a full replica for these keyspaces as soon as it joins, so do not serve reads from it until `rebuild` completes.

- [ ] **Step 4: Verify**

Run: `mvn -B verify` → `BUILD SUCCESS`. Check every command in the runbook matches what `MixedClusterIT` runs.

- [ ] **Step 5: Commit**

```bash
git add README.md NOTICE docs/migration-runbook.md
git commit -m "docs: add README, migration runbook and NOTICE"
```
