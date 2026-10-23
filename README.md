# cassandra-everywhere-strategy

A drop-in jar for Apache Cassandra 3.11 that provides `org.apache.cassandra.locator.EverywhereStrategy`, so Apache Cassandra nodes can join a mixed cluster with DataStax Enterprise (DSE) 5.1 while you migrate off DSE.

## Why

DSE keeps some of its own keyspaces (`dse_system`, `dse_insights`) on `EverywhereStrategy`, a class Apache Cassandra does not ship. A stock Cassandra 3.11 node that joins a DSE cluster cannot load that schema and logs:

```
ERROR ... Unable to merge schema from /<dse-ip>
org.apache.cassandra.exceptions.ConfigurationException: Unable to find replication strategy class 'org.apache.cassandra.locator.EverywhereStrategy'
```

You cannot work around this by altering the keyspaces: DSE rewrites `dse_system` back to `EverywhereStrategy` every time a DSE node restarts, so any change you make is undone on the next DSE restart. With this jar installed, Cassandra nodes accept that schema, stay `UN`, and log no errors across DSE restarts (verified in CI).

## Compatibility

| Component | Version | Status |
|---|---|---|
| Apache Cassandra | 3.11.19 | Tested |
| DSE | 5.1.31 | Tested |
| Other 3.11.x / 5.1.x | | Untested, expected to work |

## Install

Download the jar and its checksum from the GitHub Releases page, verify, and copy the jar into `lib/` on every Cassandra node **before it first starts**:

```bash
sha256sum -c cassandra-everywhere-strategy-cassandra-3.11-<version>.jar.sha256
cp cassandra-everywhere-strategy-cassandra-3.11-<version>.jar $CASSANDRA_HOME/lib/
```

To confirm it loaded, look for this INFO line in `system.log`. It repeats several times per schema change, which is normal:

```
EverywhereStrategy in use for keyspace <ks>
```

Releases are built by pushing a `v*` tag.

## Behaviour

- Every token-owning node in the cluster is a replica for the keyspace, in every datacenter. The replication factor of each DC equals its node count.
- Replication options are ignored. If any are given, a WARN is logged; no exception is thrown.
- A node is a full replica as soon as it joins, so it should not serve reads until its `nodetool rebuild` has finished.

## Limitations

- Joining a node with `auto_bootstrap: true` is not supported. Use `auto_bootstrap: false`, then `nodetool rebuild`. See the [migration runbook](docs/migration-runbook.md).
- While DSE nodes are in the ring, `nodetool describecluster` shows two schema versions (DSE and Cassandra). This is expected; see the runbook.
- `EACH_QUORUM` writes can fail when a datacenter has only pending (joining) nodes and no normal nodes. That happens only on unsupported paths: joining a new DC with `auto_bootstrap: true`, or replacing the only node of a DC.

## DSE 5.1.31 keyspaces that use EverywhereStrategy

On DSE 5.1.31 with only the Cassandra workload (no Search), these are:

- `dse_system`
- `dse_insights`

Check your own cluster, including any user keyspaces:

```sql
SELECT keyspace_name, replication FROM system_schema.keyspaces;
```

## Build

Building requires JDK 8 (CI uses Temurin 8): the Cassandra 3.11 classes the unit tests load do not run on newer JDKs.

```bash
mvn verify                  # unit tests
mvn -Pintegration verify    # integration tests; needs Docker
```

The integration tests start a DSE 5.1.31 node and Cassandra 3.11.19 nodes in containers. The `datastax/dse-server` image is licensed for non-production use. Override the images with `-Ddse.image=` and `-Dcassandra.image=`.

## Migration

See [docs/migration-runbook.md](docs/migration-runbook.md).

## Licence

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
