# Migration runbook: DSE 5.1 to Apache Cassandra 3.11 with EverywhereStrategy

This runbook adds an Apache Cassandra 3.11 datacenter to a live DSE 5.1 cluster, moves traffic over, and removes DSE. Every command below was exercised in CI against DSE 5.1.31 and Apache Cassandra 3.11.19 (generalised with placeholders):

- `<dse-dc>`: name of the existing DSE datacenter.
- `<new-dc>`: name of the new Cassandra datacenter.
- `<ks>`: a keyspace name.

DSE-side clean-up (for example `DseAuthenticator` and DSE-only driver protocols) is out of scope.

## 0. Prerequisites

- The Cassandra nodes use the same `cluster_name` as DSE and `endpoint_snitch: GossipingPropertyFileSnitch` (DSE's default). Set `dc` in `cassandra-rackdc.properties` to `<new-dc>`.
- The Cassandra nodes use `PasswordAuthenticator`, `CassandraAuthorizer` and `CassandraRoleManager`. Roles created under DSE internal authentication worked on them.
- Freeze DDL (schema changes, roles, grants) for the duration of the migration.
- Seeds on the Cassandra nodes point to a live node (initially the DSE nodes).

## 1. Prepare DSE

Do all DDL first. Once `system_auth` has replicas in a DC with no data, the default `cassandra` superuser cannot log in (it authenticates at QUORUM), and `CREATE ROLE` and `CREATE KEYSPACE` fail with `NoHostAvailable`.

1. Create a non-default superuser, and finish any other DDL, as `cassandra`:

   ```sql
   CREATE ROLE migrator WITH PASSWORD = '<password>' AND LOGIN = true AND SUPERUSER = true;
   ```

2. Before any Cassandra node joins, add the new DC to `system_auth`. If you skip this, joining nodes take ownership of auth ranges that have no data, and logins fail even on DSE:

   ```sql
   ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', '<dse-dc>': <n>, '<new-dc>': <m>};
   ```

   This is the last statement to run as `cassandra`. Use `migrator` from here on.

## 2. Install the jar

Copy `cassandra-everywhere-strategy-cassandra-3.11-<version>.jar` into `lib/` on every Cassandra node before it first starts (see the README for the checksum check).

## 3. Join the Cassandra nodes

Start the nodes one at a time with auto bootstrap off (the CI runs pass `-Dcassandra.auto_bootstrap=false` to the JVM). Wait for `Starting listening for CQL clients` before starting the next one. Bootstrap with `auto_bootstrap: true` is not supported.

Confirm the jar loaded:

```bash
grep "EverywhereStrategy in use for keyspace" /var/log/cassandra/system.log
```

If instead you see `Unable to merge schema from /<dse-ip>` followed by `Unable to find replication strategy class`, the jar is missing from that node's `lib/`.

Each new node is already a full replica for the EverywhereStrategy keyspaces. Do not send client reads to it until step 4 completes.

## 4. Rebuild

Run on each Cassandra node.

A whole-node `nodetool rebuild -- <dse-dc>` fails while DSE's `SimpleStrategy` keyspaces exist, with `Unable to find sufficient sources for streaming range ... in keyspace dse_perf with RF=1`. Either:

- ALTER every `SimpleStrategy` keyspace you want to keep (application keyspaces, `system_distributed`, `system_traces`, any `dse_*` you keep) to `NetworkTopologyStrategy` with both DCs, then rebuild the whole node; or
- rebuild keyspace by keyspace. CI rebuilt these four this way:

  ```bash
  nodetool rebuild -ks <ks> -- <dse-dc>
  ```

  for `system_auth`, `dse_system`, `dse_insights` and the user keyspace. Rebuild `system_auth` first; until it completes, a non-`cassandra` role cannot log in on the new node.

## 5. Verify

```bash
nodetool status
nodetool describecluster
nodetool getendpoints <ks> <table> <key>
nodetool repair -full <ks>
```

- `nodetool status`: every node is `UN`.
- `nodetool describecluster`: while DSE is in the ring this shows two schema versions, one for the DSE nodes and one shared by all Cassandra nodes. That is expected even though the schema merged fully. Check that the Cassandra nodes agree among themselves, and that `SELECT keyspace_name FROM system_schema.keyspaces` returns the same list on a Cassandra node and on DSE.
- `nodetool getendpoints` on an EverywhereStrategy keyspace lists every node in both DCs.
- Log in on a Cassandra node as the role you created on DSE: `cqlsh <cassandra-ip> -u migrator -p <password>`.
- `nodetool repair -full <ks>` prints `Repair completed successfully` (the output is long).

Restarting DSE nodes does not harm the Cassandra nodes: DSE puts `dse_system` back on EverywhereStrategy on start, and the Cassandra nodes stay `UN` without errors. Keep the jar installed until DSE is gone.

With the whole DSE DC down, `LOCAL_QUORUM` and `QUORUM` keep working in the Cassandra DC, and `ALL` fails fast with `NoHostAvailable`. This is expected.

## 6. Cut clients over

Point application clients at the Cassandra DC (local DC `<new-dc>`). Untested here beyond the CQL checks above.

## 7. Remove DSE

1. Remove each DSE node from the ring. CI ran `removenode` on a stopped DSE node; `nodetool decommission` on a live DSE node was not tested here. From a Cassandra node:

   ```bash
   nodetool removenode <dse-host-id>
   ```

   Plain `removenode` completed in CI. Get the host ID from `nodetool status`.

2. Alter every EverywhereStrategy keyspace (`dse_system`, `dse_insights`, plus any of your own) and `system_auth` to the new DC only:

   ```sql
   ALTER KEYSPACE <ks> WITH replication = {'class': 'NetworkTopologyStrategy', '<new-dc>': <rf>};
   ```

3. Run a full repair on those keyspaces:

   ```bash
   nodetool repair -full <ks>
   ```

4. Remove the jar from `lib/` on every Cassandra node:

   ```bash
   rm $CASSANDRA_HOME/lib/cassandra-everywhere-strategy*.jar
   ```

5. Update `seeds` in `cassandra.yaml` to live Cassandra nodes. Restarts with a dead DSE seed still worked in CI, but gossip logs `Received an ack from ..., who isn't a seed. Ensure your seed list includes a live node.`
6. Rolling restart, one node at a time. Wait until it shows `UN` in `nodetool status` before restarting the next.

## 8. Final checks

- `nodetool describecluster` shows one schema version.
- `grep "Unable to find replication strategy class" /var/log/cassandra/system.log` finds nothing after the restart.
- Logins and `LOCAL_QUORUM` reads work on every node.
