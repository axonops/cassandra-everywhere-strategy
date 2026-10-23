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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EverywhereStrategyOptionsTest {
    private static final String CLS = "org.apache.cassandra.locator.EverywhereStrategy";

    private Logger logger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeClass
    public static void initCassandra() {
        Ring.initCassandra();
    }

    @Before
    public void attachAppender() {
        logger = (Logger) LoggerFactory.getLogger(EverywhereStrategy.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @After
    public void detachAppender() {
        logger.detachAppender(appender);
        appender.stop();
    }

    private static Map<String, String> opts(String k, String v) {
        Map<String, String> m = new HashMap<>();
        m.put(k, v);
        return m;
    }

    private List<ILoggingEvent> events(Level level) {
        return appender.list.stream().filter(e -> e.getLevel() == level).collect(Collectors.toList());
    }

    @Test
    public void dseSchemaPushWithReplicationFactorOptionIsAccepted() throws Exception {
        Class<AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(CLS);
        AbstractReplicationStrategy s = AbstractReplicationStrategy.createReplicationStrategy(
            "ks", cls, new TokenMetadata(), Ring.SNITCH, opts("replication_factor", "3"));
        assertTrue(s instanceof EverywhereStrategy);
    }

    @Test
    public void dseSchemaPushWithNonNumericDcOptionIsAccepted() throws Exception {
        Class<AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(CLS);
        AbstractReplicationStrategy s = AbstractReplicationStrategy.createReplicationStrategy(
            "ks", cls, new TokenMetadata(), Ring.SNITCH, opts("dc1", "abc"));
        assertTrue(s instanceof EverywhereStrategy);
    }

    @Test
    public void userCreateOrAlterWithOptionsIsAccepted() throws Exception {
        Class<AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(CLS);
        AbstractReplicationStrategy.validateReplicationStrategy(
            "ks", cls, new TokenMetadata(), Ring.SNITCH, opts("dc1", "3"));
    }

    @Test
    public void optionsProduceExactlyOneWarning() throws Exception {
        Class<AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(CLS);
        AbstractReplicationStrategy.createReplicationStrategy(
            "ks", cls, new TokenMetadata(), Ring.SNITCH, opts("dc1", "3"));
        List<ILoggingEvent> warns = events(Level.WARN);
        assertEquals(1, warns.size());
        String msg = warns.get(0).getFormattedMessage();
        assertTrue(msg, msg.contains("ignores replication options"));
        assertTrue(msg, msg.contains("dc1"));
    }

    @Test
    public void noOptionsProduceNoWarningButOneInfo() throws Exception {
        Class<AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(CLS);
        AbstractReplicationStrategy.createReplicationStrategy(
            "ks", cls, new TokenMetadata(), Ring.SNITCH, Collections.<String, String>emptyMap());
        assertEquals(0, events(Level.WARN).size());
        List<ILoggingEvent> infos = events(Level.INFO);
        assertEquals(1, infos.size());
        assertTrue(infos.get(0).getFormattedMessage().contains("EverywhereStrategy in use for keyspace ks"));
    }
}
