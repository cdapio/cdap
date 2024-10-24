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
import com.google.gson.Gson;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.TaskWorker;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for {@link TaskWorkerService}.
 */
public class TaskWorkerServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();
  private static final MetricsCollectionService metricsCollectionService = new NoOpMetricsCollectionService();

  private TaskWorkerService taskWorkerService;
  private CompletableFuture<Service.State> serviceCompletionFuture;

  private CConfiguration createCConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, 0);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    cConf.set(Constants.ArtifactLocalizer.PRELOAD_LIST, "");
    cConf.setInt(Constants.ArtifactLocalizer.PRELOAD_VERSION_LIMIT, 1);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    return cConf;
  }

  private SConfiguration createSConf() {
    return SConfiguration.create();
  }

  @Before
  public void beforeTest() {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = createSConf();

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService taskWorkerService = new TaskWorkerService(
        cConf, sConf, discoveryService, discoveryService,
        metricsCollectionService,
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}));
    serviceCompletionFuture = TaskWorkerTestUtil.getServiceCompletionFuture(
        taskWorkerService);
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

  @Test
  public void testPeriodicRestart() {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = createSConf();
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 5);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService taskWorkerService = new TaskWorkerService(
        cConf, sConf, discoveryService, discoveryService,
        metricsCollectionService,
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}));
    serviceCompletionFuture = TaskWorkerTestUtil.getServiceCompletionFuture(
        taskWorkerService);
    // start the service
    taskWorkerService.startAndWait();

    TaskWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
    Assert.assertEquals(Service.State.TERMINATED, taskWorkerService.state());
  }

  @Test
  public void testPeriodicRestartWithInflightRequest() throws IOException {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = createSConf();
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 10);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 4);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService taskWorkerService = new TaskWorkerService(
        cConf, sConf, discoveryService, discoveryService,
        metricsCollectionService,
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}));
    serviceCompletionFuture = TaskWorkerTestUtil.getServiceCompletionFuture(
        taskWorkerService);
    // start the service
    taskWorkerService.startAndWait();

    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(
        String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post valid request
    String want = "5000";
    RunnableTaskRequest req = RunnableTaskRequest.getBuilder(
            TestRunnableClass.class.getName())
        .withParam(want).withNamespace("testNamespace").build();
    String reqBody = GSON.toJson(req);
    HttpResponse response = HttpRequests.execute(
        HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
            .withBody(reqBody).build(),
        new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(want, response.getResponseBodyAsString());
    TaskWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
    Assert.assertEquals(Service.State.TERMINATED, taskWorkerService.state());
  }

  @Test
  public void testPeriodicRestartWithNeverEndingInflightRequest() {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = createSConf();
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 10);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 2);
    cConf.setInt(TaskWorker.TASK_EXECUTION_DEADLINE_SECOND, -1);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService taskWorkerService =
        new TaskWorkerService(
            cConf,
            sConf,
            discoveryService,
            discoveryService,
            metricsCollectionService,
            new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}));
    serviceCompletionFuture = TaskWorkerTestUtil.getServiceCompletionFuture(
        taskWorkerService);
    // start the service
    taskWorkerService.startAndWait();

    new Thread(
        () -> {
          InetSocketAddress addr = taskWorkerService.getBindAddress();
          URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(),
              addr.getPort()));
          // Post valid request
          RunnableTaskRequest req =
              RunnableTaskRequest.getBuilder(TestRunnableClass.class.getName())
                  .withParam("200000")
                  .withNamespace("testNamespace")
                  .build();
          String reqBody = GSON.toJson(req);
          try {
            HttpRequests.execute(
                HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
                    .withBody(reqBody)
                    .build(),
                new DefaultHttpRequestConfig(false));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .start();

    TaskWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
    Assert.assertEquals(Service.State.TERMINATED, taskWorkerService.state());
  }

  @Test
  public void testRestartAfterMultipleExecutions() throws IOException {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = createSConf();
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 2);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService taskWorkerService = new TaskWorkerService(
        cConf, sConf, discoveryService, discoveryService,
        metricsCollectionService,
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}));
    serviceCompletionFuture = TaskWorkerTestUtil.getServiceCompletionFuture(
        taskWorkerService);
    // start the service
    taskWorkerService.startAndWait();

    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(
        String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post valid request
    String want = "100";
    RunnableTaskRequest req = RunnableTaskRequest.getBuilder(
            TestRunnableClass.class.getName())
        .withParam(want).withNamespace("testNamespace").build();
    String reqBody = GSON.toJson(req);
    HttpRequests.execute(
        HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
            .withBody(reqBody).build(),
        new DefaultHttpRequestConfig(false));

    HttpRequests.execute(
        HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
            .withBody(reqBody).build(),
        new DefaultHttpRequestConfig(false));

    TaskWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
    Assert.assertEquals(Service.State.TERMINATED, taskWorkerService.state());
  }

  @Test
  public void testStartAndStopWithValidRequest() throws IOException {
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(
        String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post valid request
    String want = "100";
    RunnableTaskRequest req = RunnableTaskRequest.getBuilder(
            TestRunnableClass.class.getName())
        .withParam(want).withNamespace("testNamespace").build();
    String reqBody = GSON.toJson(req);
    HttpResponse response = HttpRequests.execute(
        HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
            .withBody(reqBody).build(),
        new DefaultHttpRequestConfig(false));
    TaskWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(want, response.getResponseBodyAsString());
    Assert.assertEquals(Service.State.TERMINATED, taskWorkerService.state());
  }

  @Test
  public void testStartAndStopWithInvalidRequest() throws Exception {
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(
        String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post invalid request
    RunnableTaskRequest noClassReq = RunnableTaskRequest.getBuilder("NoClass")
        .withNamespace("testNamespace").withParam("100").build();
    String reqBody = GSON.toJson(noClassReq);
    HttpResponse response = HttpRequests.execute(
        HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
            .withBody(reqBody).build(),
        new DefaultHttpRequestConfig(false));
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
        response.getResponseCode());
    BasicThrowable basicThrowable;
    basicThrowable = GSON.fromJson(response.getResponseBodyAsString(),
        BasicThrowable.class);
    Assert.assertTrue(basicThrowable.getClassName()
        .contains("java.lang.ClassNotFoundException"));
    Assert.assertNotNull(basicThrowable.getMessage());
    Assert.assertTrue(basicThrowable.getMessage().contains("NoClass"));
    Assert.assertNotEquals(basicThrowable.getStackTraces().length, 0);
  }

  @Test
  public void testConcurrentRequestsWithIsolationEnabled() throws Exception {
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(
        String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    RunnableTaskRequest request = RunnableTaskRequest.getBuilder(
            TestRunnableClass.class.getName())
        .withParam("1000").withNamespace("testNamespace").build();

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

    List<Future<HttpResponse>> responses = Executors.newFixedThreadPool(
        concurrentRequests).invokeAll(calls);
    int okResponse = 0;
    int conflictResponse = 0;
    for (int i = 0; i < concurrentRequests; i++) {
      if (responses.get(i).get().getResponseCode()
          == HttpResponseStatus.OK.code()) {
        okResponse++;
      } else if (responses.get(i).get().getResponseCode()
                 == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
        conflictResponse++;
      }
    }
    TaskWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
    Assert.assertEquals(1, okResponse);
    Assert.assertEquals(concurrentRequests, okResponse + conflictResponse);
    Assert.assertEquals(Service.State.TERMINATED, taskWorkerService.state());
  }

  @Test
  public void testConcurrentRequestsWithIsolationDisabled() throws Exception {
    CConfiguration cConf = createCConf();
    cConf.setInt(TaskWorker.REQUEST_LIMIT, 2);
    cConf.setBoolean(TaskWorker.USER_CODE_ISOLATION_ENABLED, false);
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService taskWorkerService = new TaskWorkerService(cConf,
        createSConf(), discoveryService, discoveryService,
        metricsCollectionService,
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}));
    taskWorkerService.startAndWait();
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(
        String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    RunnableTaskRequest request = RunnableTaskRequest.getBuilder(
            TestRunnableClass.class.getName())
        .withParam("1000").withNamespace("testNamespace").build();

    String reqBody = GSON.toJson(request);
    List<Callable<HttpResponse>> calls = new ArrayList<>();
    int concurrentRequests = 3;

    for (int i = 0; i < concurrentRequests; i++) {
      calls.add(
          () -> HttpRequests.execute(
              HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
                  .withBody(reqBody).build(),
              new DefaultHttpRequestConfig(false))
      );
    }

    List<Future<HttpResponse>> responses = Executors.newFixedThreadPool(
        concurrentRequests).invokeAll(calls);
    int okResponse = 0;
    int conflictResponse = 0;
    for (int i = 0; i < concurrentRequests; i++) {
      if (responses.get(i).get().getResponseCode()
          == HttpResponseStatus.OK.code()) {
        okResponse++;
      } else if (responses.get(i).get().getResponseCode()
                 == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
        conflictResponse++;
      }
    }
    // Verify that the task worker service doesn't stop automatically.
    try {
      Tasks.waitFor(false, taskWorkerService::isRunning, 1,
          TimeUnit.SECONDS);
      Assert.fail();
    } catch (TimeoutException e) {
      // ignore.
    }
    taskWorkerService.stopAndWait();
    Assert.assertEquals(2, okResponse);
    Assert.assertEquals(concurrentRequests, okResponse + conflictResponse);
    Assert.assertEquals(Service.State.TERMINATED, taskWorkerService.state());
  }

  @Test
  public void testRestartWithConcurrentRequests() throws Exception {
    CConfiguration cConf = createCConf();
    cConf.setInt(TaskWorker.REQUEST_LIMIT, 3);
    cConf.setBoolean(TaskWorker.USER_CODE_ISOLATION_ENABLED, false);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 2);
    cConf.setInt(Constants.TaskWorker.TASK_EXECUTION_DEADLINE_SECOND, 1);
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService taskWorkerService = new TaskWorkerService(cConf,
        createSConf(), discoveryService, discoveryService,
        metricsCollectionService,
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}));
    serviceCompletionFuture = TaskWorkerTestUtil.getServiceCompletionFuture(
        taskWorkerService);
    taskWorkerService.startAndWait();
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(
        String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    List<Callable<HttpResponse>> calls = new ArrayList<>();
    int concurrentRequests = 2;

    for (int i = 0; i < concurrentRequests; i++) {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(
              TestRunnableClass.class.getName())
          .withParam("100").withNamespace("testNamespace").build();
      calls.add(
          () -> HttpRequests.execute(
              HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
                  .withBody(GSON.toJson(request)).build(),
              new DefaultHttpRequestConfig(false))
      );
    }

    // Send a request that never ends.
    RunnableTaskRequest slowRequest = RunnableTaskRequest.getBuilder(
            TestRunnableClass.class.getName())
        .withParam("1000000").withNamespace("testNamespace").build();

    calls.add(
        () -> HttpRequests.execute(
            HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
                .withBody(GSON.toJson(slowRequest)).build(),
            new DefaultHttpRequestConfig(false))
    );

    List<Future<HttpResponse>> responses = Executors.newFixedThreadPool(
        concurrentRequests).invokeAll(calls);

    int okResponse = 0;
    int connectionRefusedCount = 0;
    for (Future<HttpResponse> response : responses) {
      try {
        final int responseCode = response.get().getResponseCode();
        if (responseCode == HttpResponseStatus.OK.code()) {
          okResponse++;
        }
      } catch (ExecutionException ex) {
        if (ex.getCause() instanceof ConnectException) {
          connectionRefusedCount++;
        } else {
          throw ex;
        }
      }
    }

    // Verify that the task worker service has stopped automatically.
    Assert.assertEquals(2, okResponse);
    // The slow request will receive a "connection refused" response once the task
    // worker service stops.
    Assert.assertEquals(connectionRefusedCount, 1);
    TaskWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
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
