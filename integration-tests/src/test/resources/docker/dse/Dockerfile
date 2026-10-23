# Copyright 2026 AxonOps
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# DSE node for the mixed DSE/Cassandra integration tests.
#
# Facts checked by hand against datastax/dse-server:5.1.31 (podman):
# - Entrypoint is "/entrypoint.sh dse cassandra -f", running as user "dse"
#   (uid 999); PATH includes /opt/dse/bin (cqlsh, nodetool). DS_LICENSE=accept
#   must be set at run time or the container exits.
# - cassandra.yaml lives in /opt/dse/resources/cassandra/conf/ and already sets
#   authenticator DseAuthenticator, authorizer DseAuthorizer and role_manager
#   DseRoleManager, so it is left alone (the build only asserts those values).
#   Its endpoint_snitch (DseSimpleSnitch) is rewritten by the entrypoint from
#   $SNITCH, which defaults to GossipingPropertyFileSnitch.
# - The entrypoint applies CLUSTER_NAME, SEEDS (default: own IP), NUM_TOKENS,
#   LISTEN_ADDRESS/BROADCAST_ADDRESS (default: container IP) to cassandra.yaml,
#   and DC/RACK to cassandra-rackdc.properties.
# - Default num_tokens is 1. MAX_HEAP_SIZE/HEAP_NEWSIZE env vars are honoured
#   by cassandra-env.sh.
# - dse.yaml lives in /opt/dse/resources/dse/conf/ (owned by dse, writable);
#   its authentication/authorization/role-management blocks are all commented
#   out, i.e. auth is OFF. Internal auth is enabled by appending the blocks
#   below, mirroring production (DseAuthenticator, internal scheme).
# - The log goes to stdout and /var/log/cassandra/system.log.

ARG DSE_IMAGE=datastax/dse-server:5.1.31
FROM ${DSE_IMAGE}

RUN grep -q '^authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator' /opt/dse/resources/cassandra/conf/cassandra.yaml \
 && grep -q '^authorizer: com.datastax.bdp.cassandra.auth.DseAuthorizer' /opt/dse/resources/cassandra/conf/cassandra.yaml \
 && grep -q '^role_manager: com.datastax.bdp.cassandra.auth.DseRoleManager' /opt/dse/resources/cassandra/conf/cassandra.yaml \
 && printf '%s\n' \
      '' \
      '# Added by the everywhere-strategy integration tests.' \
      'authentication_options:' \
      '    enabled: true' \
      '    default_scheme: internal' \
      'authorization_options:' \
      '    enabled: true' \
      'role_management_options:' \
      '    mode: internal' \
      >> /opt/dse/resources/dse/conf/dse.yaml

ENV DS_LICENSE=accept \
    SNITCH=GossipingPropertyFileSnitch
