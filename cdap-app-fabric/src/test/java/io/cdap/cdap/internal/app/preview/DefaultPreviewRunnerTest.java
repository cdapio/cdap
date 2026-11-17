/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.app.preview.DataTracerFactory;
import io.cdap.cdap.app.preview.PreviewDataPublisher;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetInstanceService;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetTypeService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramNotificationSubscriberService;
import io.cdap.cdap.internal.app.services.ProgramStopSubscriberService;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultPreviewRunner}.
 */
public class DefaultPreviewRunnerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static void awaitParallelStart(CountDownLatch latch, String name)
      throws InterruptedException {
    latch.countDown();
    Assert.assertTrue("Expected parallel startup across child services for " + name,
        latch.await(5, TimeUnit.SECONDS));
  }

  private static Service createTrackingService(
      String name, CountDownLatch parallelStartLatch, List<String> stopOrder) {
    return new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        if (parallelStartLatch != null) {
          awaitParallelStart(parallelStartLatch, name);
        }
      }

      @Override
      protected void shutDown() {
        stopOrder.add(name);
      }
    };
  }

  @Test
  public void testStartUpParallelServicesAndShutDownOrder() throws Exception {
    runShutDownScenario(false);
  }

  @Test
  public void testShutDownCompletesWhenEarlierServiceFails() throws Exception {
    runShutDownScenario(true);
  }

  private void runShutDownScenario(boolean failProgramRuntimeShutDown) throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    SConfiguration sConf = SConfiguration.create();

    List<String> stopOrder = Collections.synchronizedList(new ArrayList<>());
    Service messagingBacking = createTrackingService("messaging", null, stopOrder);
    MessagingService messagingService = Mockito.mock(
        MessagingService.class,
        Mockito.withSettings().extraInterfaces(Service.class).defaultAnswer(invocation -> {
          if (Service.class.equals(invocation.getMethod().getDeclaringClass())) {
            Object result =
                invocation.getMethod().invoke(messagingBacking, invocation.getArguments());
            return result == messagingBacking ? invocation.getMock() : result;
          }
          return Mockito.RETURNS_DEFAULTS.answer(invocation);
        }));

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    CommonNettyHttpServiceFactory nettyFactory = new CommonNettyHttpServiceFactory(
        cConf, new NoOpMetricsCollectionService(), entry -> { }, new NoOpAeadCipher());

    DatasetOpExecutorService dsOpExecService = new DatasetOpExecutorService(
        cConf, sConf, discoveryService, nettyFactory, Collections.emptySet()) {
      @Override
      protected void startUp() {
        // no-op
      }

      @Override
      protected void shutDown() {
        stopOrder.add("dsOpExec");
      }
    };

    DatasetService datasetService = new DatasetService(
        cConf, sConf, discoveryService, discoveryService, nettyFactory,
        Collections.emptySet(), Mockito.mock(DatasetTypeService.class),
        Mockito.mock(DatasetInstanceService.class)) {
      @Override
      protected void doStart() {
        notifyStarted();
      }

      @Override
      protected void doStop() {
        stopOrder.add("dataset");
        notifyStopped();
      }
    };

    // All 5 services below must start concurrently before awaiting running.
    CountDownLatch parallelLatch = new CountDownLatch(5);

    TrackingMetricsService metricsService = new TrackingMetricsService(parallelLatch, stopOrder);

    ApplicationLifecycleService appLifecycleService = new ApplicationLifecycleService(
        cConf, null, null, null, null, null, null, null, null, null,
        null, null, messagingService, null, null, metricsService) {
      @Override
      protected void startUp() throws Exception {
        awaitParallelStart(parallelLatch, "appLifecycle");
      }

      @Override
      protected void shutDown() {
        stopOrder.add("appLifecycle");
      }
    };

    Service programRuntimeBacking = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        awaitParallelStart(parallelLatch, "programRuntime");
      }

      @Override
      protected void shutDown() {
        stopOrder.add("programRuntime");
        if (failProgramRuntimeShutDown) {
          throw new IllegalStateException("simulated programRuntime shutdown failure");
        }
      }
    };
    ProgramRuntimeService programRuntimeService =
        Mockito.mock(ProgramRuntimeService.class, invocation -> {
          if (Service.class.equals(invocation.getMethod().getDeclaringClass())) {
            Object result =
                invocation.getMethod().invoke(programRuntimeBacking, invocation.getArguments());
            return result == programRuntimeBacking ? invocation.getMock() : result;
          }
          return Mockito.RETURNS_DEFAULTS.answer(invocation);
        });

    ProgramNotificationSubscriberService notificationService = Mockito.mock(
        ProgramNotificationSubscriberService.class,
        Mockito.withSettings()
            .useConstructor(messagingService, cConf, metricsService, null, null,
                programRuntimeService, null, null, null, null, null)
            .defaultAnswer(invocation -> {
              String methodName = invocation.getMethod().getName();
              if ("startUp".equals(methodName)) {
                awaitParallelStart(parallelLatch, "programNotification");
                return null;
              }
              if ("shutDown".equals(methodName)) {
                stopOrder.add("programNotification");
                return null;
              }
              return invocation.callRealMethod();
            }));

    StructuredTableContext tableContext = Mockito.mock(StructuredTableContext.class);
    TransactionRunner txRunner = runnable -> {
      try {
        runnable.run(tableContext);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    ProgramStopSubscriberService stopSubscriberService = Mockito.mock(
        ProgramStopSubscriberService.class,
        Mockito.withSettings()
            .useConstructor(
                cConf, messagingService, metricsService, txRunner, programRuntimeService, null)
            .defaultAnswer(invocation -> {
              String methodName = invocation.getMethod().getName();
              if ("doStartUp".equals(methodName)) {
                awaitParallelStart(parallelLatch, "programStop");
                return null;
              }
              if ("loadMessageId".equals(methodName) && invocation.getArguments().length == 1) {
                return null;
              }
              if ("fetchMessages".equals(methodName)) {
                return Collections.emptyList();
              }
              if ("doShutdown".equals(methodName)) {
                stopOrder.add("programStop");
                return null;
              }
              return invocation.callRealMethod();
            }));

    LogAppenderInitializer logInitializer = Mockito.mock(LogAppenderInitializer.class);
    if (failProgramRuntimeShutDown) {
      Mockito.doThrow(new RuntimeException("simulated logInitializer close failure"))
          .when(logInitializer).close();
    }
    LevelDBTableService levelDbService = Mockito.mock(LevelDBTableService.class);
    StructuredTableAdmin tableAdmin = Mockito.mock(StructuredTableAdmin.class);
    Mockito.when(tableAdmin.exists(Mockito.any())).thenReturn(true);

    DefaultPreviewRunner runner = new DefaultPreviewRunner(
        messagingService,
        dsOpExecService,
        datasetService,
        logInitializer,
        appLifecycleService,
        programRuntimeService,
        Mockito.mock(ProgramLifecycleService.class),
        Mockito.mock(PreviewDataPublisher.class),
        Mockito.mock(DataTracerFactory.class),
        Mockito.mock(NamespaceAdmin.class),
        metricsService,
        notificationService,
        stopSubscriberService,
        levelDbService,
        tableAdmin,
        cConf,
        Mockito.mock(PreferencesFetcher.class));

    runner.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, messagingBacking.state());
    Assert.assertEquals(Service.State.RUNNING, dsOpExecService.state());
    Assert.assertEquals(Service.State.RUNNING, datasetService.state());
    Assert.assertEquals(Service.State.RUNNING, appLifecycleService.state());
    Assert.assertEquals(Service.State.RUNNING, programRuntimeBacking.state());
    Assert.assertEquals(Service.State.RUNNING, metricsService.state());
    Assert.assertEquals(Service.State.RUNNING, notificationService.state());
    Assert.assertEquals(Service.State.RUNNING, stopSubscriberService.state());
    Mockito.verify(logInitializer).initialize();

    runner.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, runner.state());
    Mockito.verify(logInitializer).close();
    Mockito.verify(levelDbService).close();

    Assert.assertEquals(8, stopOrder.size());
    Assert.assertEquals("programRuntime", stopOrder.get(0));
    Assert.assertEquals("appLifecycle", stopOrder.get(1));
    Assert.assertEquals("messaging", stopOrder.get(7));
  }

  private static final class TrackingMetricsService extends AbstractIdleService
      implements MetricsCollectionService {

    private final CountDownLatch parallelLatch;
    private final List<String> stopOrder;

    private TrackingMetricsService(CountDownLatch parallelLatch, List<String> stopOrder) {
      this.parallelLatch = parallelLatch;
      this.stopOrder = stopOrder;
    }

    @Override
    protected void startUp() throws Exception {
      awaitParallelStart(parallelLatch, "metrics");
    }

    @Override
    protected void shutDown() {
      stopOrder.add("metrics");
    }

    @Override
    public MetricsContext getContext(Map<String, String> tags) {
      return new NoopMetricsContext(tags);
    }
  }
}
