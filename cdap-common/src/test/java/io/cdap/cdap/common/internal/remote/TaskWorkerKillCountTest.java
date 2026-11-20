/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.gson.Gson;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.TaskWorker;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.features.Feature;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests that a task worker admits no task once a task has reached the restart count, even though
 * the worker is still up while it stops.
 */
public class TaskWorkerKillCountTest {

  private static final Gson GSON = new Gson();
  private static final AtomicInteger TASK_RUNS = new AtomicInteger();

  @Before
  public void resetTaskRuns() {
    TASK_RUNS.set(0);
  }

  @Test
  public void testRunAfterKillCountIsRejectedWhileWorkerStops() throws Exception {
    List<String> stopped = new ArrayList<>();
    // The stopper only records the call, like a worker whose server has not closed yet.
    TaskWorkerHttpHandlerInternal handler = newHandler(newCConf(), stopped::add);

    runTaskToKillCount(handler);
    Assert.assertEquals(Collections.singletonList(CountingTask.class.getName()), stopped);

    HttpResponder late = Mockito.mock(HttpResponder.class);
    handler.run(runRequest(), late);

    Mockito.verify(late).sendStatus(
        Mockito.eq(HttpResponseStatus.TOO_MANY_REQUESTS), Mockito.any(HttpHeaders.class));
    Mockito.verify(late, Mockito.never()).sendContent(Mockito.any(HttpResponseStatus.class),
        Mockito.any(BodyProducer.class), Mockito.any(HttpHeaders.class));
    Assert.assertEquals("The late task must never start", 1, TASK_RUNS.get());
    Assert.assertEquals("A rejected task must not stop the worker again", 1, stopped.size());
  }

  @Test
  public void testRejectionAfterKillCountReportsTerminating() throws Exception {
    CConfiguration cConf = newCConf();
    cConf.setBoolean("feature." + Feature.TASK_WORKER_MANAGER.getFeatureFlagString(), true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    TaskWorkerHttpHandlerInternal handler = newHandler(cConf, className -> { });

    runTaskToKillCount(handler);

    HttpResponder late = Mockito.mock(HttpResponder.class);
    handler.run(runRequest(), late);

    // TERMINATING makes the task worker manager back off this worker instead of retrying it.
    ArgumentCaptor<HttpHeaders> headers = ArgumentCaptor.forClass(HttpHeaders.class);
    Mockito.verify(late).sendStatus(
        Mockito.eq(HttpResponseStatus.TOO_MANY_REQUESTS), headers.capture());
    Assert.assertEquals(TaskWorkerStatus.TERMINATING.name(),
        headers.getValue().get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
    Assert.assertEquals("The late task must never start", 1, TASK_RUNS.get());
  }

  /** Runs one task and finishes writing its response, which reaches the kill count of one. */
  private static void runTaskToKillCount(TaskWorkerHttpHandlerInternal handler) throws Exception {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    handler.run(runRequest(), responder);

    ArgumentCaptor<BodyProducer> producer = ArgumentCaptor.forClass(BodyProducer.class);
    Mockito.verify(responder).sendContent(Mockito.eq(HttpResponseStatus.OK), producer.capture(),
        Mockito.any(HttpHeaders.class));
    Assert.assertEquals(1, TASK_RUNS.get());
    producer.getValue().finished();
  }

  private static CConfiguration newCConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(TaskWorker.USER_CODE_ISOLATION_ENABLED, true);
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    // Keep the periodic restart thread, and the System.exit it ends in, out of the test.
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);
    return cConf;
  }

  private static TaskWorkerHttpHandlerInternal newHandler(CConfiguration cConf,
      Consumer<String> stopper) {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    return new TaskWorkerHttpHandlerInternal(cConf, discoveryService, discoveryService, stopper,
        new NoOpMetricsCollectionService());
  }

  private static FullHttpRequest runRequest() {
    String body = GSON.toJson(RunnableTaskRequest.getBuilder(CountingTask.class.getName())
        .withParam("param")
        .withNamespace("ns1")
        .build());
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
        "/v3Internal/worker/run", Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }

  /** Counts how many times a task ran. */
  public static class CountingTask implements RunnableTask {

    @Override
    public void run(RunnableTaskContext context) throws Exception {
      TASK_RUNS.incrementAndGet();
      context.writeResult("done".getBytes(StandardCharsets.UTF_8));
    }
  }
}
