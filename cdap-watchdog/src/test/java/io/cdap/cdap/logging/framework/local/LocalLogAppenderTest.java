/*
 * Copyright © 2024 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.logging.framework.local;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.pipeline.MockAppender;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LocalLogAppender} start/stop lifecycle and pipeline thread naming.
 */
public class LocalLogAppenderTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testStartRestartThreadNamesAndStopLifecycle() throws Exception {
    File pipelineDir = TMP_FOLDER.newFolder();
    String customPipelineName = "customLocalPipe";
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<configuration>\n"
        + "  <contextName>" + customPipelineName + "</contextName>\n"
        + "  <appender name=\"MOCK\" class=\"" + MockAppender.class.getName() + "\"/>\n"
        + "  <root level=\"INFO\">\n"
        + "    <appender-ref ref=\"MOCK\"/>\n"
        + "  </root>\n"
        + "</configuration>\n";
    Files.write(new File(pipelineDir, customPipelineName + ".xml").toPath(),
        xml.getBytes(StandardCharsets.UTF_8));

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Logging.PIPELINE_CONFIG_DIR, pipelineDir.getAbsolutePath());
    cConf.setLong(Constants.Logging.PIPELINE_EVENT_DELAY_MS, 50L);

    TransactionRunner txRunner = Mockito.mock(TransactionRunner.class);
    LocalLocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());

    LocalLogAppender appender = new LocalLogAppender(
        cConf, txRunner, locationFactory, new NoOpMetricsCollectionService());
    appender.setContext(new LoggerContext());

    appender.start();
    Assert.assertTrue(appender.isStarted());

    Set<Thread> firstThreads = getPipelineThreads(appender);
    Assert.assertEquals(2, firstThreads.size());
    Assert.assertTrue(firstThreads.stream()
        .anyMatch(t -> "LocalLogProcessor-cdap".equals(t.getName())));
    Assert.assertTrue(firstThreads.stream()
        .anyMatch(t -> ("LocalLogProcessor-" + customPipelineName).equals(t.getName())));

    // Restarting stops previous pipelines via stopAndWait and creates new pipeline threads
    appender.start();
    Tasks.waitFor(true, () -> firstThreads.stream().noneMatch(Thread::isAlive),
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

    Set<Thread> secondThreads = getPipelineThreads(appender);
    Assert.assertEquals(2, secondThreads.size());

    LoggingContextAccessor.setLoggingContext(
        new WorkerLoggingContext("default", "app1", "worker1", "run1", "0"));
    LoggingEvent event = new LoggingEvent();
    event.setLevel(Level.INFO);
    event.setLoggerName("io.cdap.LocalTest");
    event.setMessage("local pipeline message");
    event.setTimeStamp(System.currentTimeMillis());
    appender.doAppend(event);

    appender.stop();
    Assert.assertFalse(appender.isStarted());
    Assert.assertTrue(getPipelineThreads(appender).isEmpty());
    Tasks.waitFor(true, () -> secondThreads.stream().noneMatch(Thread::isAlive),
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unchecked")
  private static Set<Thread> getPipelineThreads(LocalLogAppender appender) throws Exception {
    Field field = LocalLogAppender.class.getDeclaredField("pipelineThreads");
    field.setAccessible(true);
    AtomicReference<Set<Thread>> ref = (AtomicReference<Set<Thread>>) field.get(appender);
    return ref.get();
  }
}
