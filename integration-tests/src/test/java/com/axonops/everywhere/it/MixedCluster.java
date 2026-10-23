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
package com.axonops.everywhere.it;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Network.Ipam;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * A mixed cluster: one DSE node (DC {@code dc1}) and up to two Apache Cassandra nodes (DC {@code dc2}) on a
 * dedicated Docker network with fixed addresses ({@code <subnet>.10} for DSE, {@code <subnet>.20},
 * {@code <subnet>.21} for Cassandra). Fixed addresses let tests restart containers and keep their identity.
 *
 * <p>{@link #start()} starts only the DSE node; Cassandra nodes are started one at a time by
 * {@link #joinCassandra(int)} or {@link #startCassandraExpectingFailure(int)}. {@link #close()} writes each
 * container's log to {@code target/container-logs/<subnet>-<name>.log} and removes everything.
 */
public final class MixedCluster implements AutoCloseable {

    public static final String CLUSTER_NAME = "everywhere-it";
    public static final String DSE_DC = "dc1";
    public static final String CASSANDRA_DC = "dc2";
    public static final String RACK = "r1";

    static final String IP_LABEL = "com.axonops.everywhere.it.ip";
    static final String NAME_LABEL = "com.axonops.everywhere.it.name";
    private static final String CQL_READY = "Starting listening for CQL clients";
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(10);
    private static final int MAX_CASSANDRA_NODES = 2;

    private final String dseImage;
    private final String cassandraImage;
    private final int cassandraNodes;
    private final boolean withJar;
    private final String subnet;

    private Network network;
    private GenericContainer<?> dse;
    private final List<GenericContainer<?>> cassandra = new ArrayList<>();

    private MixedCluster(Builder b) {
        this.dseImage = b.dseImage;
        this.cassandraImage = b.cassandraImage;
        this.cassandraNodes = b.cassandraNodes;
        this.withJar = b.withJar;
        this.subnet = b.subnet;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Creates the network, starts the DSE node and waits until {@code cassandra}/{@code cassandra} can log in. */
    public MixedCluster start() {
        try {
            startDse();
            return this;
        } catch (RuntimeException | Error e) {
            // try-with-resources never sees a cluster whose start() failed, so save logs and clean up here.
            close();
            throw e;
        }
    }

    private void startDse() {
        network = Network.builder()
                .createNetworkCmdModifier(cmd -> cmd.withIpam(new Ipam()
                        .withConfig(new Ipam.Config().withSubnet(subnet + ".0/24"))))
                .build();

        dse = node(dseImage(), "dse", subnet + ".10")
                .withEnv("DS_LICENSE", "accept")
                .withEnv("CLUSTER_NAME", CLUSTER_NAME)
                .withEnv("DC", DSE_DC)
                .withEnv("RACK", RACK)
                .withEnv("SEEDS", seed())
                .withEnv("SNITCH", "GossipingPropertyFileSnitch")
                .waitingFor(Wait.forLogMessage(".*" + CQL_READY + ".*", 1).withStartupTimeout(STARTUP_TIMEOUT));
        dse.start();
        Shell.awaitCqlLogin(dse, "cassandra", "cassandra", Duration.ofMinutes(3));

        ImageFromDockerfile image = cassandraImage();
        for (int i = 0; i < cassandraNodes; i++) {
            cassandra.add(node(image, "cassandra-" + i, subnet + "." + (20 + i))
                    .withEnv("CASSANDRA_CLUSTER_NAME", CLUSTER_NAME)
                    .withEnv("CASSANDRA_SEEDS", seed())
                    .withEnv("CASSANDRA_DC", CASSANDRA_DC)
                    .withEnv("CASSANDRA_RACK", RACK)
                    .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
                    .withEnv("JVM_EXTRA_OPTS", "-Dcassandra.auto_bootstrap=false"));
        }
    }

    public GenericContainer<?> dse() {
        return dse;
    }

    /** Cassandra nodes in index order; a node is only running once joined or started. */
    public List<GenericContainer<?>> cassandra() {
        return Collections.unmodifiableList(cassandra);
    }

    /** Starts Cassandra node {@code index} (0-based) and waits until it listens for CQL clients. */
    public GenericContainer<?> joinCassandra(int index) {
        GenericContainer<?> node = cassandra.get(index);
        node.waitingFor(Wait.forLogMessage(".*" + CQL_READY + ".*", 1).withStartupTimeout(STARTUP_TIMEOUT));
        node.start();
        return node;
    }

    /** Starts Cassandra node {@code index} (0-based) without waiting for it to become usable. */
    public GenericContainer<?> startCassandraExpectingFailure(int index) {
        GenericContainer<?> node = cassandra.get(index);
        node.waitingFor(new AbstractWaitStrategy() {
            @Override
            protected void waitUntilReady() {
                // Deliberately nothing: the caller checks the log for the expected failure.
            }
        });
        node.start();
        return node;
    }

    public String ip(GenericContainer<?> c) {
        return ipOf(c);
    }

    /**
     * Stops (if running) and starts the same container, keeping its address and data, then waits until it
     * logs that it listens for CQL clients again.
     */
    public void restart(GenericContainer<?> c) {
        int before = count(c.getLogs(), CQL_READY);
        if (isRunning(c)) {
            stop(c);
        }
        docker().startContainerCmd(c.getContainerId()).exec();
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (count(c.getLogs(), CQL_READY) <= before) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(nameOf(c) + " did not listen for CQL clients within " + STARTUP_TIMEOUT
                        + " after restart");
            }
            sleep(2000);
        }
    }

    /** Stops the container without removing it, so {@link #restart} can bring it back with the same address. */
    public void stop(GenericContainer<?> c) {
        docker().stopContainerCmd(c.getContainerId()).withTimeout(60).exec();
    }

    @Override
    public void close() {
        List<GenericContainer<?>> all = new ArrayList<>(cassandra);
        if (dse != null) {
            all.add(dse);
        }
        for (GenericContainer<?> c : all) {
            if (c.getContainerId() == null) {
                continue;
            }
            try {
                writeLog(c);
            } catch (RuntimeException | IOException e) {
                System.err.println("Could not save the log of " + nameOf(c) + ": " + e);
            }
            try {
                c.stop();
            } catch (RuntimeException e) {
                System.err.println("Could not remove " + nameOf(c) + ": " + e);
            }
        }
        if (network != null) {
            network.close();
        }
    }

    static String ipOf(GenericContainer<?> c) {
        String ip = c.getLabels().get(IP_LABEL);
        if (ip == null) {
            throw new IllegalArgumentException("Not a MixedCluster container: " + c);
        }
        return ip;
    }

    private static String nameOf(GenericContainer<?> c) {
        return c.getLabels().getOrDefault(NAME_LABEL, String.valueOf(c.getContainerId()));
    }

    private String seed() {
        return subnet + ".10";
    }

    @SuppressWarnings("resource")
    private GenericContainer<?> node(ImageFromDockerfile image, String name, String ip) {
        return new GenericContainer<>(image)
                .withNetwork(network)
                .withCreateContainerCmdModifier(cmd -> cmd.withIpv4Address(ip))
                .withLabel(IP_LABEL, ip)
                .withLabel(NAME_LABEL, name)
                .withEnv("MAX_HEAP_SIZE", "1G")
                .withEnv("HEAP_NEWSIZE", "256M");
    }

    private ImageFromDockerfile dseImage() {
        return new ImageFromDockerfile()
                .withFileFromClasspath("Dockerfile", "docker/dse/Dockerfile")
                .withBuildArg("DSE_IMAGE", dseImage);
    }

    private ImageFromDockerfile cassandraImage() {
        ImageFromDockerfile image = new ImageFromDockerfile()
                .withFileFromClasspath("Dockerfile", "docker/cassandra/Dockerfile")
                .withBuildArg("CASSANDRA_IMAGE", cassandraImage);
        if (!withJar) {
            return image.withTarget("nojar");
        }
        String jar = System.getProperty("everywhere.jar");
        if (jar == null || !Files.isRegularFile(Paths.get(jar))) {
            throw new IllegalStateException("System property everywhere.jar must name the built jar, got: " + jar
                    + " (run with mvn verify -Pintegration)");
        }
        return image.withFileFromPath("everywhere.jar", Paths.get(jar)).withTarget("withjar");
    }

    private void writeLog(GenericContainer<?> c) throws IOException {
        Path dir = Paths.get(System.getProperty("container.logs.dir", "target/container-logs"));
        Files.createDirectories(dir);
        Files.write(dir.resolve(subnet + "-" + nameOf(c) + ".log"), c.getLogs().getBytes(StandardCharsets.UTF_8));
    }

    private static boolean isRunning(GenericContainer<?> c) {
        Boolean running = docker().inspectContainerCmd(c.getContainerId()).exec().getState().getRunning();
        return Boolean.TRUE.equals(running);
    }

    private static DockerClient docker() {
        return DockerClientFactory.instance().client();
    }

    private static int count(String haystack, String needle) {
        int n = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            n++;
        }
        return n;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting", e);
        }
    }

    public static final class Builder {
        private String dseImage = System.getProperty("dse.image", "datastax/dse-server:5.1.31");
        private String cassandraImage = System.getProperty("cassandra.image", "cassandra:3.11.19");
        private int cassandraNodes = 1;
        private boolean withJar = true;
        private String subnet = "172.30.0";

        private Builder() {
        }

        public Builder dseImage(String image) {
            this.dseImage = image;
            return this;
        }

        public Builder cassandraImage(String image) {
            this.cassandraImage = image;
            return this;
        }

        /** Number of Cassandra nodes (1 or 2). */
        public Builder cassandraNodes(int n) {
            if (n < 1 || n > MAX_CASSANDRA_NODES) {
                throw new IllegalArgumentException("cassandraNodes must be 1.." + MAX_CASSANDRA_NODES + ", got " + n);
            }
            this.cassandraNodes = n;
            return this;
        }

        /** Whether the Cassandra nodes get the EverywhereStrategy jar (default true). */
        public Builder withJar(boolean withJar) {
            this.withJar = withJar;
            return this;
        }

        /**
         * First three octets of the /24 subnet for this cluster's network, e.g. {@code "172.30.1"} (default
         * {@code "172.30.0"}). Give each test class its own subnet.
         */
        public Builder subnet(String subnet) {
            if (!subnet.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
                throw new IllegalArgumentException("subnet must be three octets like 172.30.1, got " + subnet);
            }
            this.subnet = subnet;
            return this;
        }

        public MixedCluster build() {
            return new MixedCluster(this);
        }
    }
}
