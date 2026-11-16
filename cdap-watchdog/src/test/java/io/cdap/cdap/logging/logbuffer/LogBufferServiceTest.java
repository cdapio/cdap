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

package io.cdap.cdap.logging.logbuffer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Provider;
import io.cdap.cdap.api.logging.AppenderContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.framework.LocalAppenderContext;
import io.cdap.cdap.logging.pipeline.MockAppender;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LogBufferService}, covering fast-completing recovery startup, discovery
 * registration, and stopping remaining pipelines when one pipeline fails.
 */
public class LogBufferServiceTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testFastCompletingRecoveryStartAndStopLifecycle() throws Exception {
    File nonExistentBufferDir = new File(TMP_FOLDER.newFolder(), "non-existent-buf");
    File pipelineDir = TMP_FOLDER.newFolder();
    writePipelineConfig(pipelineDir, "extraBufPipe");

    CConfiguration cConf = createBaseConf(nonExistentBufferDir, pipelineDir);
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    Set<String> started = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    Set<String> stopped = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    LogBufferService service = new LogBufferService(
        cConf,
        SConfiguration.create(),
        discoveryService,
        Collections.emptySet(),
        createContextProvider(started, stopped, Collections.emptySet()),
        new CommonNettyHttpServiceFactory(
            cConf, new NoOpMetricsCollectionService(), entry -> { }, null),
        createNoOpTxRunner());

    // LogBufferRecoveryService finishes run() immediately because nonExistentBufferDir does not exist
    service.startAndWait();
    Assert.assertFalse(Iterables.isEmpty(discoveryService.discover(Constants.Service.LOGSAVER)));
    Tasks.waitFor(true, () -> started.contains("cdap") && started.contains("extraBufPipe"),
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

    service.stopAndWait();
    Assert.assertTrue(Iterables.isEmpty(discoveryService.discover(Constants.Service.LOGSAVER)));
    Assert.assertTrue(stopped.contains("cdap"));
    Assert.assertTrue(stopped.contains("extraBufPipe"));
  }

  @Test
  public void testRecoveryWithExistingBufferedLogsAndPipelineFailureCleanup() throws Exception {
    File bufferDir = TMP_FOLDER.newFolder();
    LoggingEventSerializer serializer = new LoggingEventSerializer();
    LoggingEvent rawEvent = new LoggingEvent();
    rawEvent.setLevel(Level.INFO);
    rawEvent.setLoggerName("io.cdap.RecoverTest");
    rawEvent.setMessage("recovered-event");
    rawEvent.setTimeStamp(System.currentTimeMillis());
    byte[] encoded = serializer.toBytes(
        new LogMessage(rawEvent, new WorkerLoggingContext("default", "app1", "w1", "r1", "0")));
    try (LogBufferWriter preWriter = new LogBufferWriter(
        bufferDir.getAbsolutePath(), 100000L, () -> { })) {
      preWriter.write(ImmutableList.of(encoded).iterator());
    }

    File pipelineDir = TMP_FOLDER.newFolder();
    writePipelineConfig(pipelineDir, "failingBufPipe");

    CConfiguration cConf = createBaseConf(bufferDir, pipelineDir);
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    Set<String> started = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    Set<String> stopped = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    LogBufferService service = new LogBufferService(
        cConf,
        SConfiguration.create(),
        discoveryService,
        Collections.emptySet(),
        createContextProvider(started, stopped, Collections.singleton("failingBufPipe")),
        new CommonNettyHttpServiceFactory(
            cConf, new NoOpMetricsCollectionService(), entry -> { }, null),
        createNoOpTxRunner());

    service.startAndWait();
    Tasks.waitFor(true, () -> started.contains("cdap"),
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

    try {
      service.stopAndWait();
    } catch (Exception ignored) {
      // If a future fix propagates pipeline failure on shutdown, allow it as long as remaining
      // healthy pipelines are still stopped.
    }

    Assert.assertTrue(stopped.contains("cdap"));
    Assert.assertTrue(Iterables.isEmpty(discoveryService.discover(Constants.Service.LOGSAVER)));
  }

  private static CConfiguration createBaseConf(File bufferDir, File pipelineDir) throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.LogBuffer.LOG_BUFFER_BASE_DIR, bufferDir.getAbsolutePath());
    cConf.setLong(Constants.LogBuffer.LOG_BUFFER_MAX_FILE_SIZE_BYTES, 100000L);
    cConf.setInt(Constants.LogBuffer.LOG_BUFFER_RECOVERY_BATCH_SIZE, 10);
    cConf.set(Constants.Logging.PIPELINE_CONFIG_DIR, pipelineDir.getAbsolutePath());
    cConf.setLong(Constants.Logging.PIPELINE_BUFFER_SIZE, 1024L);
    cConf.set(Constants.LogSaver.ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.LogSaver.PORT, 0);
    cConf.set("system.log.process.retry.policy.type", "none");
    return cConf;
  }

  private static Provider<AppenderContext> createContextProvider(
      Set<String> started, Set<String> stopped, Set<String> failingNames) {
    return () -> {
      File dir;
      try {
        dir = TMP_FOLDER.newFolder();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return new LocalAppenderContext(
          createNoOpTxRunner(), new LocalLocationFactory(dir), new NoOpMetricsCollectionService()) {
        @Override
        public int getInstanceId() {
          return 1;
        }

        @Override
        public void start() {
          if (failingNames.contains(getName())) {
            throw new IllegalStateException("pipeline start failed: " + getName());
          }
          super.start();
          started.add(getName());
        }

        @Override
        public void stop() {
          super.stop();
          stopped.add(getName());
        }
      };
    };
  }

  private static TransactionRunner createNoOpTxRunner() {
    StructuredTable table = Mockito.mock(StructuredTable.class);
    StructuredTableContext ctx = Mockito.mock(StructuredTableContext.class);
    try {
      Mockito.when(table.read(Mockito.anyCollection())).thenReturn(Optional.empty());
      Mockito.when(ctx.getTable(Mockito.any())).thenReturn(table);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
    return runnable -> {
      try {
        runnable.run(ctx);
      } catch (Exception e) {
        throw new io.cdap.cdap.spi.data.transaction.TransactionException("tx error", e);
      }
    };
  }

  private static void writePipelineConfig(File dir, String contextName) throws Exception {
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<configuration>\n"
        + "  <contextName>" + contextName + "</contextName>\n"
        + "  <appender name=\"MOCK\" class=\"" + MockAppender.class.getName() + "\"/>\n"
        + "  <root level=\"INFO\">\n"
        + "    <appender-ref ref=\"MOCK\"/>\n"
        + "  </root>\n"
        + "</configuration>\n";
    Files.write(new File(dir, contextName + ".xml").toPath(), xml.getBytes(StandardCharsets.UTF_8));
  }
}
