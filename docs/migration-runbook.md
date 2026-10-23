# Migration runbook: DSE 5.1 to Apache Cassandra 3.11 with EverywhereStrategy

This runbook adds an Apache Cassandra 3.11 datacenter to a live DSE 5.1 cluster, moves traffic over, and removes DSE. The sequence is based on the one verified in CI (`MixedClusterIT`) against DSE 5.1.31 and Apache Cassandra 3.11.19, generalised with placeholders. Steps marked "recommended, not exercised in CI" are standard add-/remove-DC practice that the CI run did not cover:

- `<dse-dc>`: name of the existing DSE datacenter.
- `<new-dc>`: name of the new Cassandra datacenter.
- `<ks>`: a keyspace name.

Paths below (`$CASSANDRA_HOME/lib`, `/var/log/cassandra/system.log`) vary by install; adjust them for yours. For example, package installs use `/usr/share/cassandra/lib` and `/var/log/cassandra`.

DSE-side clean-up (for example `DseAuthenticator` and DSE-only driver protocols) is out of scope.

## 0. Prerequisites

- The Cassandra nodes use the same `cluster_name` as DSE and `endpoint_snitch: GossipingPropertyFileSnitch` (DSE's default). Set `dc` in `cassandra-rackdc.properties` to `<new-dc>`.
- The Cassandra nodes use `PasswordAuthenticator`, `CassandraAuthorizer` and `CassandraRoleManager`. Roles created under DSE internal authentication worked on them.
- Freeze schema and application changes (keyspaces, tables, roles, grants) for the duration of the migration, apart from the steps below.
- Pause scheduled and automatic repairs (for example an OpsCenter repair service) for the duration of the migration. Run only the explicit `nodetool repair -full` steps below.
- Seeds on the Cassandra nodes point to a live node (initially the DSE nodes).
- Until clients are cut over in step 6, clients must use `LOCAL_*` consistency levels (`LOCAL_ONE`, `LOCAL_QUORUM`, `LOCAL_SERIAL`) with their local DC pinned to `<dse-dc>`. From the moment a keyspace gets replicas in `<new-dc>` until that keyspace's rebuild has finished, those replicas are empty; `QUORUM`, `EACH_QUORUM`, `SERIAL` and `ALL` would count them.

## 1. Prepare DSE

1. Create a non-default superuser as `cassandra`, and finish all other DDL:

   ```sql
   CREATE ROLE migrator WITH PASSWORD = '<password>' AND LOGIN = true AND SUPERUSER = true;
   ```

   Use `migrator` from step 4 on. The default `cassandra` superuser authenticates at `QUORUM`, so it cannot log in while `system_auth` has empty replicas in `<new-dc>`; `CREATE ROLE` and the permission grant inside `CREATE KEYSPACE` fail then too.

2. Before any Cassandra node joins, alter every `SimpleStrategy` keyspace you will keep to `NetworkTopologyStrategy` with `<dse-dc>` only, at the same replication factor. That is your application keyspaces, `system_auth`, `system_distributed`, `system_traces` and any `dse_*` keyspace you keep:

   ```sql
   ALTER KEYSPACE <ks> WITH replication = {'class': 'NetworkTopologyStrategy', '<dse-dc>': <n>};
   ```

   `SimpleStrategy` ignores datacenters, so without this the joining nodes take ownership of ranges of these keyspaces that hold no data on them. For `system_auth` that means logins fail even on DSE. CI ran this for `system_auth` (`{'<dse-dc>': 1}`). Keyspaces that are already `NetworkTopologyStrategy` with `<dse-dc>` only need nothing here.

## 2. Install the jar

Copy `cassandra-everywhere-strategy-cassandra-3.11-<version>.jar` into `lib/` on every Cassandra node before it first starts (see the README for the checksum check).

## 3. Join the Cassandra nodes

Before a node first starts:

- Set `num_tokens` in `cassandra.yaml` deliberately. It cannot be changed once the node has joined. The Cassandra 3.11 default is 256, while DSE nodes often use 1 (check `nodetool status` or `num_tokens` on DSE). The token count also multiplies the cost of range calculations for every keyspace.
- Set auto bootstrap off. `auto_bootstrap` is not in the default `cassandra.yaml`, so add this line:

  ```yaml
  auto_bootstrap: false
  ```

  The equivalent JVM option, which is what CI used, is `-Dcassandra.auto_bootstrap=false`. Bootstrap (`auto_bootstrap: true`) is not a supported path: if a node was started with the default by accident, stop it rather than letting it bootstrap.

Start the nodes one at a time and wait for `Starting listening for CQL clients`, and for the node to show `UN` in `nodetool status`, before starting the next one.

Confirm the jar loaded:

```bash
grep "EverywhereStrategy in use for keyspace" /var/log/cassandra/system.log
```

If instead you see `Unable to merge schema from /<dse-ip>` followed by `Unable to find replication strategy class`, the jar is missing from that node's `lib/`.

Each new node is already a full replica for the EverywhereStrategy keyspaces. Do not send client reads to it until step 4 completes.

## 4. Add the new DC to the keyspaces and rebuild

Run the `ALTER`s as `migrator` on DSE. Run each rebuild on one Cassandra node at a time: start it on the next node only after it has finished on the previous one.

1. `system_auth` first. Add `<new-dc>`, then immediately rebuild it on every Cassandra node:

   ```sql
   ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', '<dse-dc>': <n>, '<new-dc>': <m>};
   ```

   ```bash
   nodetool rebuild -ks system_auth -- <dse-dc>
   ```

   Until it completes on a node, roles cannot log in on that node. CI ran exactly this.

2. Add `<new-dc>` to every other keyspace that you converted or that already uses `NetworkTopologyStrategy` (application keyspaces, `system_distributed`, `system_traces`, any `dse_*` you keep), then go straight on to rebuilding them in the next step (recommended, not exercised in CI: CI's application keyspace uses EverywhereStrategy):

   ```sql
   ALTER KEYSPACE <ks> WITH replication = {'class': 'NetworkTopologyStrategy', '<dse-dc>': <n>, '<new-dc>': <m>};
   ```

3. Rebuild every keyspace that should hold data in the new DC, keyspace by keyspace. This includes the EverywhereStrategy keyspaces (`dse_system`, `dse_insights` and any of your own), which need no `ALTER`. On each Cassandra node in turn:

   ```bash
   for ks in <ks1> <ks2> system_distributed system_traces dse_system dse_insights; do
     nodetool rebuild -ks "$ks" -- <dse-dc> || break
   done
   ```

   List only the keyspaces you kept. CI rebuilt `system_auth`, `dse_system`, `dse_insights` and its EverywhereStrategy application keyspace this way.

Notes:

- Check that the `ALTER` has reached the node before rebuilding there, for example `nodetool getendpoints <ks> <table> <key>` lists `<new-dc>` nodes. For a keyspace that lists `<dse-dc>` only, `nodetool rebuild -ks` exits 0 and streams nothing.
- A whole-node `nodetool rebuild -- <dse-dc>` fails while any `SimpleStrategy` keyspace exists, including the RF 1 DSE keyspaces you are not keeping (`dse_perf`, `dse_leases`, `dse_security`): `Unable to find sufficient sources for streaming range ... in keyspace dse_perf with RF=1`. Use it only after converting every `SimpleStrategy` keyspace.

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

EverywhereStrategy keyspaces use a per-DC replication factor equal to the DC's node count, so `LOCAL_QUORUM` in the Cassandra DC needs a majority of the Cassandra nodes. CI checked that with one of two Cassandra nodes down, `LOCAL_QUORUM` there fails with `Unavailable` and `LOCAL_ONE` still works.

With the whole DSE DC down, `LOCAL_QUORUM` keeps working in the Cassandra DC, and `ALL` fails fast with `NoHostAvailable`. This is expected. `QUORUM` also worked in CI, but only because the Cassandra DC held a majority of all nodes (1 DSE node, 2 Cassandra nodes). If the DSE DC has as many nodes as the Cassandra DC or more, `QUORUM` fails while DSE is down.

## 6. Cut clients over

Point application clients at the Cassandra DC (local DC `<new-dc>`). Untested here beyond the CQL checks above.

## 7. Remove DSE

1. Make sure every client uses `<new-dc>` as its local DC (step 6).

2. While DSE is still up, drop `<dse-dc>` from your application keyspaces and from the other `NetworkTopologyStrategy` keyspaces you keep (`system_distributed`, `system_traces`, any `dse_*` you keep). Recommended, not exercised in CI (generic remove-DC practice):

   ```sql
   ALTER KEYSPACE <ks> WITH replication = {'class': 'NetworkTopologyStrategy', '<new-dc>': <rf>};
   ```

3. Retire the DSE nodes, one at a time. Get host IDs from `nodetool status`.

   - A live DSE node: run `nodetool decommission` on it. Not tested here.
   - A dead DSE node: from a Cassandra node, run:

     ```bash
     nodetool removenode <dse-host-id>
     ```

     CI ran this on a stopped DSE node and it completed.

4. Alter every EverywhereStrategy keyspace (`dse_system`, `dse_insights`, plus any of your own) and `system_auth` to the new DC only. CI ran this after `removenode`:

   ```sql
   ALTER KEYSPACE <ks> WITH replication = {'class': 'NetworkTopologyStrategy', '<new-dc>': <rf>};
   ```

   Do this only once DSE is gone: a DSE node that starts puts `dse_system` back on EverywhereStrategy.

5. Check that no keyspace still uses EverywhereStrategy:

   ```sql
   SELECT keyspace_name, replication FROM system_schema.keyspaces;
   ```

   Do not remove the jar while any row shows `org.apache.cassandra.locator.EverywhereStrategy`: the next restart would fail with `Unable to find replication strategy class`.

6. Run a full repair on the altered keyspaces (recommended, not exercised in CI):

   ```bash
   nodetool repair -full <ks>
   ```

7. Remove the jar from `lib/` on every Cassandra node. CI removed the jar file and restarted both nodes without it:

   ```bash
   rm $CASSANDRA_HOME/lib/cassandra-everywhere-strategy*.jar
   ```

8. Update `seeds` in `cassandra.yaml` to live Cassandra nodes (recommended, not exercised in CI). Restarts with only a dead DSE seed still worked in CI, but gossip then logs `Received an ack from ..., who isn't a seed. Ensure your seed list includes a live node.`

9. Rolling restart, one node at a time. Wait until it shows `UN` in `nodetool status` before restarting the next.

## 8. Final checks

- `nodetool describecluster` shows one schema version.
- `grep "Unable to find replication strategy class" /var/log/cassandra/system.log` finds nothing after the restart.
- Logins and `LOCAL_QUORUM` reads work on every node.
- Resume scheduled repairs.
