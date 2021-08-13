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

package io.cdap.cdap.internal.app.worker;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Unit test for {@link TaskWorkerService}.
 */
public class TaskWorkerServiceTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerServiceTest.class);
  private static final Gson GSON = new Gson();

  private TaskWorkerService taskWorkerService;

  private CConfiguration createCConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, 0);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    cConf.set(Constants.TaskWorker.PRELOAD_ARTIFACTS, "");
    return cConf;
  }

  private SConfiguration createSConf() {
    return SConfiguration.create();
  }

  @Before
  public void beforeTest() {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = createSConf();

    TaskWorkerService taskWorkerService = new TaskWorkerService(cConf, sConf, new InMemoryDiscoveryService(),
                                                                (namespaceId, retryStrategy) -> null);
    // start the service
    taskWorkerService.startAndWait();
    this.taskWorkerService = taskWorkerService;
  }

  @After
  public void afterTest() {
    if (taskWorkerService != null) {
      taskWorkerService.stopAndWait();
      taskWorkerService = null;
    }
  }

  private void waitForTaskWorkerToFinish(TaskWorkerService taskWorker) {
    CompletableFuture<Service.State> future = new CompletableFuture<>();
    taskWorker.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        future.complete(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    try {
      Uninterruptibles.getUninterruptibly(future);
      LOG.debug("Task worker stopped");
    } catch (Exception e) {
      LOG.warn("Task worker stopped with exception", e);
    }
  }

  @Test
  public void testStartAndStopWithValidRequest() throws IOException {
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post valid request
    String want = "100";
    RunnableTaskRequest req = RunnableTaskRequest.getBuilder(TestRunnableClass.class.getName()).withParam(want).build();
    String reqBody = GSON.toJson(req);
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
        .withBody(reqBody).build(),
      new DefaultHttpRequestConfig(false));
    waitForTaskWorkerToFinish(taskWorkerService);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(want, response.getResponseBodyAsString());
    Assert.assertEquals(Service.State.TERMINATED, taskWorkerService.state());
  }

  @Test
  public void testStartAndStopWithInvalidRequest() throws Exception {
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post invalid request
    RunnableTaskRequest noClassReq = RunnableTaskRequest.getBuilder("NoClass").build();
    String reqBody = GSON.toJson(noClassReq);
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
        .withBody(reqBody).build(),
      new DefaultHttpRequestConfig(false));
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getResponseCode());
    BasicThrowable basicThrowable;
    basicThrowable = GSON.fromJson(response.getResponseBodyAsString(), BasicThrowable.class);
    Assert.assertTrue(basicThrowable.getClassName().contains("java.lang.ClassNotFoundException"));
    Assert.assertNotNull(basicThrowable.getMessage());
    Assert.assertTrue(basicThrowable.getMessage().contains("NoClass"));
    Assert.assertNotEquals(basicThrowable.getStackTraces().length, 0);
  }

  @Test
  public void testConcurrentRequests() throws Exception {
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    RunnableTaskRequest request = RunnableTaskRequest.getBuilder(TestRunnableClass.class.getName()).
      withParam("1000").build();

    String reqBody = GSON.toJson(request);
    List<Callable<HttpResponse>> calls = new ArrayList<>();
    int concurrentRequests = 2;

    for (int i = 0; i < concurrentRequests; i++) {
      calls.add(
        () -> HttpRequests.execute(
          HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
            .withBody(reqBody).build(),
          new DefaultHttpRequestConfig(false))
      );
    }

    List<Future<HttpResponse>> responses = Executors.newFixedThreadPool(concurrentRequests).invokeAll(calls);
    int okResponse = 0;
    int conflictResponse = 0;
    for (int i = 0; i < concurrentRequests; i++) {
      if (responses.get(i).get().getResponseCode() == HttpResponseStatus.OK.code()) {
        okResponse++;
      } else if (responses.get(i).get().getResponseCode() == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
        conflictResponse++;
      }
    }
    waitForTaskWorkerToFinish(taskWorkerService);
    Assert.assertEquals(1, okResponse);
    Assert.assertEquals(concurrentRequests, okResponse + conflictResponse);
    Assert.assertEquals(Service.State.TERMINATED, taskWorkerService.state());
  }

  public static class TestRunnableClass implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      if (!context.getParam().equals("")) {
        Thread.sleep(Integer.parseInt(context.getParam()));
      }
      context.writeResult(context.getParam().getBytes(StandardCharsets.UTF_8));
    }
  }
}
