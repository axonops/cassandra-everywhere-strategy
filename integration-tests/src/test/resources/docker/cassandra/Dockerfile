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

# Apache Cassandra node for the mixed DSE/Cassandra integration tests.
#
# Facts checked by hand against cassandra:3.11.19 (podman):
# - CASSANDRA_CONF is /etc/cassandra; /etc/cassandra/cassandra.yaml exists and
#   defaults to AllowAllAuthenticator / AllowAllAuthorizer /
#   CassandraRoleManager, endpoint_snitch SimpleSnitch, num_tokens 256.
# - Jars on the classpath come from /opt/cassandra/lib/ (owned by root).
# - The entrypoint applies CASSANDRA_CLUSTER_NAME, CASSANDRA_SEEDS,
#   CASSANDRA_DC, CASSANDRA_RACK, CASSANDRA_ENDPOINT_SNITCH; cassandra-env.sh
#   honours MAX_HEAP_SIZE, HEAP_NEWSIZE and JVM_EXTRA_OPTS
#   (-Dcassandra.auto_bootstrap=false is passed that way at run time).
#
# Two targets: "nojar" (stock Cassandra, used by the baseline-failure test)
# and "withjar" (EverywhereStrategy jar added to /opt/cassandra/lib/).
# The build context must contain everywhere.jar for the "withjar" target.

ARG CASSANDRA_IMAGE=cassandra:3.11.19
FROM ${CASSANDRA_IMAGE} AS nojar

RUN sed -i \
      -e 's/^authenticator:.*/authenticator: PasswordAuthenticator/' \
      -e 's/^authorizer:.*/authorizer: CassandraAuthorizer/' \
      -e 's/^role_manager:.*/role_manager: CassandraRoleManager/' \
      /etc/cassandra/cassandra.yaml \
 && grep -q '^authenticator: PasswordAuthenticator' /etc/cassandra/cassandra.yaml \
 && grep -q '^authorizer: CassandraAuthorizer' /etc/cassandra/cassandra.yaml \
 && grep -q '^role_manager: CassandraRoleManager' /etc/cassandra/cassandra.yaml

FROM nojar AS withjar
COPY everywhere.jar /opt/cassandra/lib/cassandra-everywhere-strategy.jar
