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
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultPreviewManager}.
 */
public class DefaultPreviewManagerTest {

  @Test
  public void testStartUpAndShutDownLifecycle() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.Preview.DATA_CLEANUP_INTERVAL_SECONDS, 60L);

    StructuredTableAdmin tableAdmin = Mockito.mock(StructuredTableAdmin.class);
    Mockito.when(tableAdmin.exists(Mockito.any())).thenReturn(true);

    AtomicBoolean appenderStarted = new AtomicBoolean(false);
    AtomicBoolean appenderStopped = new AtomicBoolean(false);
    LogAppender logAppender = new LogAppender() {
      @Override
      protected void appendEvent(LogMessage logMessage) {
        // no-op
      }

      @Override
      public void start() {
        appenderStarted.set(true);
      }

      @Override
      public void stop() {
        appenderStopped.set(true);
      }
    };

    MessagingService messagingService = Mockito.mock(MessagingService.class);
    TestMetricsService metricsService = new TestMetricsService(false);
    PreviewStore previewStore = Mockito.mock(PreviewStore.class);
    PreviewRunStopper runStopper = Mockito.mock(PreviewRunStopper.class);

    FakeLogSubscriber logSubscriber =
        new FakeLogSubscriber(cConf, messagingService, metricsService, false);
    FakeDataSubscriber dataSubscriber = new FakeDataSubscriber(
        cConf, messagingService, metricsService, previewStore, runStopper, false);
    FakeCleanupService cleanupService = new FakeCleanupService(cConf, previewStore, false);
    LevelDBTableService levelDbService = Mockito.mock(LevelDBTableService.class);

    Injector previewInjector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(StructuredTableAdmin.class).toInstance(tableAdmin);
        bind(LogAppender.class).toInstance(logAppender);
        bind(PreviewTMSLogSubscriber.class).toInstance(logSubscriber);
        bind(PreviewDataSubscriberService.class).toInstance(dataSubscriber);
      }
    });

    DefaultPreviewManager manager = new DefaultPreviewManager(
        Mockito.mock(DiscoveryServiceClient.class),
        Mockito.mock(DatasetFramework.class),
        Mockito.mock(TransactionSystemClient.class),
        Mockito.mock(AccessControllerInstantiator.class),
        Mockito.mock(AccessEnforcer.class),
        Mockito.mock(AuthenticationContext.class),
        levelDbService,
        cConf,
        new Configuration(),
        SConfiguration.create(),
        Mockito.mock(PreviewRequestQueue.class),
        previewStore,
        runStopper,
        messagingService,
        cleanupService,
        metricsService) {
      @Override
      Injector createPreviewInjector() {
        return previewInjector;
      }
    };

    manager.startAsync().awaitRunning();
    Tasks.waitFor(Service.State.RUNNING, metricsService::state, 5, TimeUnit.SECONDS,
        20, TimeUnit.MILLISECONDS);
    Assert.assertTrue(appenderStarted.get());
    Assert.assertEquals(Service.State.RUNNING, logSubscriber.state());
    Assert.assertEquals(Service.State.RUNNING, dataSubscriber.state());
    Assert.assertEquals(Service.State.RUNNING, cleanupService.state());

    manager.stopAsync().awaitTerminated();
    Assert.assertTrue(appenderStopped.get());
    Assert.assertEquals(Service.State.TERMINATED, cleanupService.state());
    Assert.assertEquals(Service.State.TERMINATED, dataSubscriber.state());
    Assert.assertEquals(Service.State.TERMINATED, logSubscriber.state());
    Assert.assertEquals(Service.State.TERMINATED, metricsService.state());
    Mockito.verify(levelDbService).close();
  }

  @Test
  public void testStopQuietlyContinuesWhenChildServiceStopFails() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    StructuredTableAdmin tableAdmin = Mockito.mock(StructuredTableAdmin.class);
    Mockito.when(tableAdmin.exists(Mockito.any())).thenReturn(true);

    LogAppender logAppender = Mockito.mock(LogAppender.class);
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    TestMetricsService metricsService = new TestMetricsService(true);
    PreviewStore previewStore = Mockito.mock(PreviewStore.class);
    PreviewRunStopper runStopper = Mockito.mock(PreviewRunStopper.class);

    FakeLogSubscriber logSubscriber =
        new FakeLogSubscriber(cConf, messagingService, metricsService, false);
    // dataSubscriber fails during shutDown; stopQuietly must still stop remaining services.
    FakeDataSubscriber dataSubscriber = new FakeDataSubscriber(
        cConf, messagingService, metricsService, previewStore, runStopper, true);
    FakeCleanupService cleanupService = new FakeCleanupService(cConf, previewStore, false);
    LevelDBTableService levelDbService = Mockito.mock(LevelDBTableService.class);

    Injector previewInjector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(StructuredTableAdmin.class).toInstance(tableAdmin);
        bind(LogAppender.class).toInstance(logAppender);
        bind(PreviewTMSLogSubscriber.class).toInstance(logSubscriber);
        bind(PreviewDataSubscriberService.class).toInstance(dataSubscriber);
      }
    });

    DefaultPreviewManager manager = new DefaultPreviewManager(
        Mockito.mock(DiscoveryServiceClient.class),
        Mockito.mock(DatasetFramework.class),
        Mockito.mock(TransactionSystemClient.class),
        Mockito.mock(AccessControllerInstantiator.class),
        Mockito.mock(AccessEnforcer.class),
        Mockito.mock(AuthenticationContext.class),
        levelDbService,
        cConf,
        new Configuration(),
        SConfiguration.create(),
        Mockito.mock(PreviewRequestQueue.class),
        previewStore,
        runStopper,
        messagingService,
        cleanupService,
        metricsService) {
      @Override
      Injector createPreviewInjector() {
        return previewInjector;
      }
    };

    manager.startAsync().awaitRunning();
    manager.stopAsync().awaitTerminated();

    Assert.assertEquals(Service.State.TERMINATED, cleanupService.state());
    Assert.assertEquals(Service.State.FAILED, dataSubscriber.state());
    Assert.assertEquals(Service.State.TERMINATED, logSubscriber.state());
    Mockito.verify(logAppender).stop();
    Mockito.verify(levelDbService).close();
  }

  private static TransactionRunner newNoopTxRunner() {
    StructuredTableContext tableContext = Mockito.mock(StructuredTableContext.class);
    return runnable -> {
      try {
        runnable.run(tableContext);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  private static final class FakeLogSubscriber extends PreviewTMSLogSubscriber {

    private final boolean failOnStop;

    private FakeLogSubscriber(CConfiguration cConf, MessagingService messagingService,
        MetricsCollectionService metricsService, boolean failOnStop) {
      super(cConf, messagingService, metricsService, newNoopTxRunner(), null);
      this.failOnStop = failOnStop;
    }

    @Nullable
    @Override
    protected String loadMessageId(StructuredTableContext context) {
      return null;
    }

    @Override
    protected List<Message> fetchMessages(@Nullable String messageId) {
      return Collections.emptyList();
    }

    @Override
    protected void doShutdown() {
      if (failOnStop) {
        throw new IllegalStateException("log subscriber stop failure");
      }
    }
  }

  private static final class FakeDataSubscriber extends PreviewDataSubscriberService {

    private final boolean failOnStop;

    private FakeDataSubscriber(CConfiguration cConf, MessagingService messagingService,
        MetricsCollectionService metricsService, PreviewStore previewStore,
        PreviewRunStopper runStopper, boolean failOnStop) {
      super(cConf, messagingService, metricsService, previewStore, newNoopTxRunner(), runStopper);
      this.failOnStop = failOnStop;
    }

    @Nullable
    @Override
    protected String loadMessageId(StructuredTableContext context) {
      return null;
    }

    @Override
    protected List<Message> fetchMessages(@Nullable String messageId) {
      return Collections.emptyList();
    }

    @Override
    protected void doShutdown() {
      if (failOnStop) {
        throw new IllegalStateException("data subscriber stop failure");
      }
    }
  }

  private static final class FakeCleanupService extends PreviewDataCleanupService {

    private final boolean failOnStop;

    private FakeCleanupService(CConfiguration cConf, PreviewStore previewStore,
        boolean failOnStop) {
      super(cConf, previewStore);
      this.failOnStop = failOnStop;
    }

    @Override
    protected void runOneIteration() {
      // no-op
    }

    @Override
    protected void shutDown() throws Exception {
      super.shutDown();
      if (failOnStop) {
        throw new IllegalStateException("cleanup stop failure");
      }
    }
  }

  private static final class TestMetricsService extends AbstractIdleService
      implements MetricsCollectionService {

    private final boolean failOnShutDown;

    private TestMetricsService(boolean failOnShutDown) {
      this.failOnShutDown = failOnShutDown;
    }

    @Override
    protected void startUp() {
      // no-op
    }

    @Override
    protected void shutDown() {
      if (failOnShutDown) {
        throw new IllegalStateException("metrics stop failure");
      }
    }

    @Override
    public MetricsContext getContext(Map<String, String> tags) {
      return new NoopMetricsContext(tags);
    }
  }
}
