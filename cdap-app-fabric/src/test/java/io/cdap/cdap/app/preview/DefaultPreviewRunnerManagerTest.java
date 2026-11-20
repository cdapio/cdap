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

package io.cdap.cdap.app.preview;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.internal.app.preview.PreviewRequestFetcher;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultPreviewRunnerManager}.
 */
public class DefaultPreviewRunnerManagerTest {

  private DefaultPreviewRunnerManager createManager(CConfiguration cConf,
      PreviewRunner previewRunner, PreviewRequestFetcher fetcher) {
    Injector factoryInjector = Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(CConfiguration.class).toInstance(cConf);
            bind(PreviewRequestFetcher.class).toInstance(fetcher);
            install(new FactoryModuleBuilder().build(PreviewRunnerServiceFactory.class));
          }
        });
    PreviewRunnerServiceFactory serviceFactory =
        factoryInjector.getInstance(PreviewRunnerServiceFactory.class);

    Injector previewInjector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(PreviewRunner.class).toInstance(previewRunner);
      }
    });

    return new DefaultPreviewRunnerManager(
        cConf,
        new Configuration(),
        SConfiguration.create(),
        Mockito.mock(SecureStore.class),
        Mockito.mock(DiscoveryServiceClient.class),
        Mockito.mock(DatasetFramework.class),
        Mockito.mock(TransactionSystemClient.class),
        Mockito.mock(PreviewRunnerModule.class),
        Mockito.mock(LevelDBTableService.class),
        serviceFactory) {
      @Override
      public Injector createPreviewInjector() {
        return previewInjector;
      }
    };
  }

  @Test
  public void testAutoStopWhenAllPollerServicesTerminate() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Preview.POLLER_COUNT, 2);
    cConf.setInt(Constants.Preview.MAX_RUNS, 1);
    cConf.setLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS, 20L);

    FakePreviewRunnerService previewRunner = new FakePreviewRunnerService(false);
    QueuePreviewRequestFetcher fetcher = new QueuePreviewRequestFetcher();
    DefaultPreviewRunnerManager manager = createManager(cConf, previewRunner, fetcher);

    manager.startAndWait();
    Assert.assertEquals(Service.State.RUNNING, previewRunner.state());
    Assert.assertEquals(Service.State.RUNNING, manager.state());

    ProgramId firstProgram =
        NamespaceId.DEFAULT.app("preview1").program(ProgramType.WORKFLOW, "wf");
    ProgramId secondProgram =
        NamespaceId.DEFAULT.app("preview2").program(ProgramType.WORKFLOW, "wf");
    fetcher.enqueue(new PreviewRequest(firstProgram, null, null));
    fetcher.enqueue(new PreviewRequest(secondProgram, null, null));

    Tasks.waitFor(true, () -> previewRunner.activeFutures.containsKey(firstProgram)
        && previewRunner.activeFutures.containsKey(secondProgram),
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

    // Completing the first preview stops only 1 of 2 pollers; manager stays RUNNING.
    previewRunner.complete(firstProgram);
    Assert.assertEquals(Service.State.RUNNING, manager.state());

    // Completing the second preview terminates the last poller, triggering auto-stop on manager.
    previewRunner.complete(secondProgram);
    Tasks.waitFor(Service.State.TERMINATED, manager::state, 5, TimeUnit.SECONDS,
        20, TimeUnit.MILLISECONDS);
    Assert.assertEquals(Service.State.TERMINATED, previewRunner.state());
  }

  @Test
  public void testStopPreviewReplacesPollerWithoutStoppingManager() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Preview.POLLER_COUNT, 1);
    cConf.setLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS, 20L);

    FakePreviewRunnerService previewRunner = new FakePreviewRunnerService(false);
    QueuePreviewRequestFetcher fetcher = new QueuePreviewRequestFetcher();
    DefaultPreviewRunnerManager manager = createManager(cConf, previewRunner, fetcher);

    manager.startAndWait();
    try {
      ApplicationId unknownApp = NamespaceId.DEFAULT.app("missing");
      Assert.assertThrows(NotFoundException.class, () -> manager.stop(unknownApp));

      ProgramId runningProgram =
          NamespaceId.DEFAULT.app("previewActive").program(ProgramType.WORKFLOW, "wf");
      fetcher.enqueue(new PreviewRequest(runningProgram, null, null));
      Tasks.waitFor(true, () -> previewRunner.activeFutures.containsKey(runningProgram),
          5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

      final int fetchesBeforeStop = fetcher.fetchCount.get();
      manager.stop(runningProgram.getParent());
      Assert.assertTrue(previewRunner.stoppedPrograms.getOrDefault(runningProgram, false));
      Assert.assertEquals(Service.State.RUNNING, manager.state());

      // Replacement poller service should actively poll for subsequent requests.
      Tasks.waitFor(true, () -> fetcher.fetchCount.get() > fetchesBeforeStop,
          5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
    } finally {
      manager.stopAndWait();
    }
    Assert.assertEquals(Service.State.TERMINATED, previewRunner.state());
  }

  @Test
  public void testShutDownStopsQuietlyWhenPreviewRunnerFailsToStop() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Preview.POLLER_COUNT, 1);
    cConf.setLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS, 20L);

    FakePreviewRunnerService failingStopRunner = new FakePreviewRunnerService(true);
    QueuePreviewRequestFetcher fetcher = new QueuePreviewRequestFetcher();
    DefaultPreviewRunnerManager manager = createManager(cConf, failingStopRunner, fetcher);

    manager.startAndWait();
    Assert.assertEquals(Service.State.RUNNING, failingStopRunner.state());

    // stopQuietly should swallow the exception from failingStopRunner.shutDown().
    manager.stopAndWait();
    Assert.assertEquals(Service.State.TERMINATED, manager.state());
    Assert.assertTrue(failingStopRunner.shutDownCalled.get());
  }

  private static final class FakePreviewRunnerService extends AbstractIdleService
      implements PreviewRunner {

    private final boolean failOnShutDown;
    private final AtomicBoolean shutDownCalled = new AtomicBoolean(false);
    private final Map<ProgramId, CompletableFuture<PreviewRequest>> activeFutures =
        new ConcurrentHashMap<>();
    private final Map<ProgramId, Boolean> stoppedPrograms = new ConcurrentHashMap<>();

    private FakePreviewRunnerService(boolean failOnShutDown) {
      this.failOnShutDown = failOnShutDown;
    }

    @Override
    protected void startUp() {
      // no-op
    }

    @Override
    protected void shutDown() {
      shutDownCalled.set(true);
      if (failOnShutDown) {
        throw new IllegalStateException("runner shutdown error");
      }
    }

    @Override
    public Future<PreviewRequest> startPreview(PreviewRequest previewRequest) {
      CompletableFuture<PreviewRequest> future = new CompletableFuture<>();
      activeFutures.put(previewRequest.getProgram(), future);
      return future;
    }

    @Override
    public void stopPreview(ProgramId programId) {
      stoppedPrograms.put(programId, true);
      CompletableFuture<PreviewRequest> future = activeFutures.get(programId);
      if (future != null) {
        future.complete(null);
      }
    }

    private void complete(ProgramId programId) {
      CompletableFuture<PreviewRequest> future = activeFutures.get(programId);
      if (future != null) {
        future.complete(null);
      }
    }
  }

  private static final class QueuePreviewRequestFetcher implements PreviewRequestFetcher {

    private final Queue<PreviewRequest> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger fetchCount = new AtomicInteger();

    @Override
    public Optional<PreviewRequest> fetch() {
      fetchCount.incrementAndGet();
      return Optional.ofNullable(queue.poll());
    }

    private void enqueue(PreviewRequest request) {
      queue.offer(request);
    }
  }
}
