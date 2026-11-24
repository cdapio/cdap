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

package io.cdap.cdap.logging.framework.distributed;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provider;
import io.cdap.cdap.api.logging.AppenderContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.logging.framework.LocalAppenderContext;
import io.cdap.cdap.logging.pipeline.MockAppender;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.zookeeper.ZKClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Fast unit tests for composite pipeline lifecycle and failure aggregation in
 * {@link DistributedLogFramework}.
 */
public class DistributedLogFrameworkPipelineTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCompositeServiceHappyPathStartAndStop() throws Exception {
    File pipelineDir = TMP_FOLDER.newFolder();
    writePipelineConfig(pipelineDir, "extra1");
    writePipelineConfig(pipelineDir, "extra2");

    CConfiguration cConf = createBaseConf(pipelineDir);
    Set<String> startedContexts = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    Set<String> stoppedContexts = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    DistributedLogFramework framework = createFramework(
        cConf, createTrackingProvider(startedContexts, stoppedContexts, Collections.emptySet()));

    Service compositeService = framework.createService(Collections.singleton(0));
    compositeService.startAsync().awaitRunning();
    Tasks.waitFor(true, () -> startedContexts.containsAll(ImmutableSet.of("cdap", "extra1", "extra2")),
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

    compositeService.stopAsync().awaitTerminated();
    Assert.assertTrue(stoppedContexts.containsAll(ImmutableSet.of("cdap", "extra1", "extra2")));
  }

  @Test
  public void testMultiPipelineFailuresAggregateSuppressedAndStopRemaining() throws Exception {
    File pipelineDir = TMP_FOLDER.newFolder();
    writePipelineConfig(pipelineDir, "failPipe1");
    writePipelineConfig(pipelineDir, "failPipe2");

    CConfiguration cConf = createBaseConf(pipelineDir);
    Set<String> startedContexts = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    Set<String> stoppedContexts = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    Set<String> failingPipelines = ImmutableSet.of("failPipe1", "failPipe2");

    Map<String, LocalAppenderContext> contextsByName = new ConcurrentHashMap<>();
    Provider<AppenderContext> provider = () -> {
      LocalAppenderContext ctx = createContext(
          startedContexts, stoppedContexts, failingPipelines);
      return ctx;
    };

    DistributedLogFramework framework = createFramework(cConf, () -> {
      LocalAppenderContext ctx = (LocalAppenderContext) provider.get();
      return new LocalAppenderContext(
          createNoOpTxRunner(),
          new org.apache.twill.filesystem.LocalLocationFactory(createTempDirUnchecked()),
          new NoOpMetricsCollectionService()) {
        @Override
        public int getInstanceId() {
          return 1;
        }

        @Override
        public void start() {
          contextsByName.put(getName(), this);
          if (failingPipelines.contains(getName())) {
            throw new IllegalStateException("startup failure in " + getName());
          }
          super.start();
          startedContexts.add(getName());
        }

        @Override
        public void stop() {
          super.stop();
          stoppedContexts.add(getName());
        }
      };
    });

    Service compositeService = framework.createService(Collections.singleton(0));
    compositeService.startAsync().awaitRunning();

    // Wait for healthy cdap pipeline to start and both failing RetryOnStartFailureService wrappers
    // to transition to FAILED (since retry policy is none).
    Tasks.waitFor(true, () -> startedContexts.contains("cdap") && contextsByName.size() == 3,
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
    TimeUnit.MILLISECONDS.sleep(100);

    try {
      compositeService.stopAsync().awaitTerminated();
      Assert.fail("Expected composite service stopAndWait to fail when two pipelines failed");
    } catch (RuntimeException expected) {
      Throwable failureCause = expected.getCause();
      Assert.assertNotNull(failureCause);
      Assert.assertEquals(1, failureCause.getSuppressed().length);
    }

    // Remaining healthy pipeline ("cdap") must still be stopped cleanly.
    Assert.assertTrue(stoppedContexts.contains("cdap"));
  }

  private static CConfiguration createBaseConf(File pipelineDir) throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Logging.PIPELINE_CONFIG_DIR, pipelineDir.getAbsolutePath());
    cConf.set(Constants.Logging.KAFKA_TOPIC, "logs");
    cConf.setInt(Constants.Logging.NUM_PARTITIONS, 1);
    cConf.setLong(Constants.Logging.PIPELINE_BUFFER_SIZE, 1024L);
    cConf.set("system.log.process.retry.policy.type", "none");
    return cConf;
  }

  private static DistributedLogFramework createFramework(
      CConfiguration cConf, Provider<AppenderContext> provider) {
    return new DistributedLogFramework(
        cConf,
        Mockito.mock(ZKClient.class),
        Mockito.mock(DiscoveryService.class),
        Mockito.mock(DiscoveryServiceClient.class),
        provider,
        Mockito.mock(BrokerService.class),
        createNoOpTxRunner());
  }

  private static Provider<AppenderContext> createTrackingProvider(
      Set<String> started, Set<String> stopped, Set<String> failingNames) {
    return () -> createContext(started, stopped, failingNames);
  }

  private static LocalAppenderContext createContext(
      Set<String> started, Set<String> stopped, Set<String> failingNames) {
    return new LocalAppenderContext(
        createNoOpTxRunner(),
        new org.apache.twill.filesystem.LocalLocationFactory(createTempDirUnchecked()),
        new NoOpMetricsCollectionService()) {
      @Override
      public int getInstanceId() {
        return 1;
      }

      @Override
      public void start() {
        if (failingNames.contains(getName())) {
          throw new IllegalStateException("simulated failure in " + getName());
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

  private static File createTempDirUnchecked() {
    try {
      return TMP_FOLDER.newFolder();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
