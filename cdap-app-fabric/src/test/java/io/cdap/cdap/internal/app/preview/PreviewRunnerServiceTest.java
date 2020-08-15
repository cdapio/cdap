/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.util.concurrent.Service;
import com.google.gson.JsonElement;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.metrics.query.MetricsQueryHelper;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit test for {@link PreviewRunnerService}.
 */
public class PreviewRunnerServiceTest {

  private CConfiguration createCConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS, 200);
    return cConf;
  }

  @Test
  public void testStartAndStop() throws InterruptedException, ExecutionException, TimeoutException {
    MockPreviewRunner mockRunner = new MockPreviewRunner();
    MockPreviewRequestFetcher fetcher = new MockPreviewRequestFetcher();
    PreviewRunnerService runnerService = new PreviewRunnerService(createCConf(), mockRunner, fetcher);
    runnerService.startAndWait();

    Tasks.waitFor(true, () -> fetcher.fetchCount.get() > 0, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    runnerService.stopAndWait();
    Tasks.waitFor(Service.State.TERMINATED, runnerService::state, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testStopPreview() throws InterruptedException, ExecutionException, TimeoutException {
    MockPreviewRunner mockRunner = new MockPreviewRunner();
    MockPreviewRequestFetcher fetcher = new MockPreviewRequestFetcher();
    PreviewRunnerService runnerService = new PreviewRunnerService(createCConf(), mockRunner, fetcher);
    runnerService.startAndWait();

    ProgramId programId = NamespaceId.DEFAULT.app("app").program(ProgramType.WORKFLOW, "workflow");
    fetcher.addRequest(new PreviewRequest(programId, null));

    Tasks.waitFor(true, () -> mockRunner.requests.get(programId.getParent()) != null,
                  5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    runnerService.stopAndWait();
    Tasks.waitFor(PreviewStatus.Status.KILLED, () -> mockRunner.requests.get(programId.getParent()).status.getStatus(),
                  5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    Tasks.waitFor(Service.State.TERMINATED, runnerService::state, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testMaxRuns() throws InterruptedException, ExecutionException, TimeoutException, NotFoundException {
    CConfiguration cConf = createCConf();
    cConf.setInt(Constants.Preview.MAX_RUNS, 1);

    MockPreviewRunner mockRunner = new MockPreviewRunner();
    MockPreviewRequestFetcher fetcher = new MockPreviewRequestFetcher();
    PreviewRunnerService runnerService = new PreviewRunnerService(cConf, mockRunner, fetcher);
    runnerService.startAndWait();

    ProgramId programId = NamespaceId.DEFAULT.app("app").program(ProgramType.WORKFLOW, "workflow");
    fetcher.addRequest(new PreviewRequest(programId, null));
    Tasks.waitFor(true, () -> mockRunner.requests.get(programId.getParent()) != null,
                  50, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Finish the preview run and the runner service should be completed as well since max runs == 1
    mockRunner.complete(programId.getParent());
    Tasks.waitFor(Service.State.TERMINATED, runnerService::state, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  /**
   * A mocking {@link PreviewRunner} for unit testing.
   */
  private static final class MockPreviewRunner implements PreviewRunner {

    private final Map<ApplicationId, RequestInfo> requests = new ConcurrentHashMap<>();

    @Override
    public Future<PreviewRequest> startPreview(PreviewRequest request) {
      CompletableFuture<PreviewRequest> future = new CompletableFuture<>();
      requests.put(request.getProgram().getParent(),
                   new RequestInfo(request, future, new PreviewStatus(PreviewStatus.Status.RUNNING,
                                                                      null, System.currentTimeMillis(), null)));
      return future;
    }

    @Override
    public PreviewStatus getStatus(ApplicationId applicationId) throws NotFoundException {
      RequestInfo info = requests.get(applicationId);
      if (info == null) {
        throw new NotFoundException(applicationId);
      }
      return info.status;
    }

    @Override
    public void stopPreview(ApplicationId applicationId) throws Exception {
      RequestInfo info = requests.get(applicationId);
      if (info == null) {
        throw new NotFoundException(applicationId);
      }
      info.status = new PreviewStatus(PreviewStatus.Status.KILLED, null,
                                      info.status.getStartTime(), System.currentTimeMillis());
      info.future.complete(info.request);
    }

    @Override
    public Map<String, List<JsonElement>> getData(ApplicationId applicationId, String tracerName) {
      return Collections.emptyMap();
    }

    @Override
    public RunRecordDetail getRunRecord(ApplicationId applicationId) throws Exception {
      return null;
    }

    @Override
    public MetricsQueryHelper getMetricsQueryHelper() {
      return null;
    }

    void complete(ApplicationId applicationId) throws NotFoundException {
      RequestInfo info = requests.get(applicationId);
      if (info == null) {
        throw new NotFoundException(applicationId);
      }
      info.future.complete(info.request);
    }

    private static final class RequestInfo {
      private final PreviewRequest request;
      private final CompletableFuture<PreviewRequest> future;
      private PreviewStatus status;

      private RequestInfo(PreviewRequest request, CompletableFuture<PreviewRequest> future, PreviewStatus status) {
        this.request = request;
        this.future = future;
        this.status = status;
      }
    }
  }

  /**
   * A mocking {@link PreviewRequestFetcher} for unit testing.
   */
  private static final class MockPreviewRequestFetcher implements PreviewRequestFetcher {

    private final Queue<PreviewRequest> requests = new ConcurrentLinkedQueue<>();
    private final AtomicInteger fetchCount = new AtomicInteger();

    @Override
    public Optional<PreviewRequest> fetch() {
      fetchCount.incrementAndGet();
      return Optional.ofNullable(requests.poll());
    }

    void addRequest(PreviewRequest request) {
      requests.offer(request);
    }
  }
}
