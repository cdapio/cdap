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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import io.cdap.cdap.api.task.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
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
  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerServiceTest.class);
  private static final Gson GSON = new Gson();


  private CConfiguration createCConf(int port) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, port);
    cConf.setLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS, 200);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    return cConf;
  }

  private SConfiguration createSConf() {
    SConfiguration sConf = SConfiguration.create();
    return sConf;
  }

  private TaskWorkerService setupTaskWorkerService(int port) {
    CConfiguration cConf = createCConf(port);
    SConfiguration sConf = createSConf();

    TaskWorkerService taskWorkerService = new TaskWorkerService(cConf, sConf, new InMemoryDiscoveryService());
    // start the service
    taskWorkerService.startAndWait();

    return taskWorkerService;
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
      LOG.debug("task worker stopped");
    } catch (Exception e) {
      LOG.warn("Task worker stopped with exception", e);
    }
  }

  @Test
  public void testStartAndStopWithValidRequest() throws IOException {
    TaskWorkerService taskWorkerService = setupTaskWorkerService(10001);
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Get request
    HttpResponse response = HttpRequests.execute(HttpRequest.get(uri.resolve("/v3Internal/worker/get").toURL()).build(),
                                                 new DefaultHttpRequestConfig(false));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());


    // Post valid request
    String want = "100";
    RunnableTaskRequest req = new RunnableTaskRequest(TestRunnableClass.class.getName(), want);
    String reqBody = GSON.toJson(req);
    response = HttpRequests.execute(
      HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
        .withBody(reqBody).build(),
      new DefaultHttpRequestConfig(false));
    waitForTaskWorkerToFinish(taskWorkerService);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(want, response.getResponseBodyAsString());
    Assert.assertTrue(taskWorkerService.state() == Service.State.TERMINATED);
  }

  @Test
  public void testStartAndStopWithInvalidRequest() throws Exception {
    TaskWorkerService taskWorkerService = setupTaskWorkerService(10002);
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post invalid request
    RunnableTaskRequest noClassReq = new RunnableTaskRequest("NoClass", "");
    String reqBody = GSON.toJson(noClassReq);
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
        .withBody(reqBody).build(),
      new DefaultHttpRequestConfig(false));
    waitForTaskWorkerToFinish(taskWorkerService);
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString().contains("java.lang.ClassNotFoundException"));
    Assert.assertTrue(taskWorkerService.state() == Service.State.TERMINATED);
  }

  @Test
  public void testConcurrentRequests() throws Exception {
    TaskWorkerService taskWorkerService = setupTaskWorkerService(10003);
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    RunnableTaskRequest request = new RunnableTaskRequest(TestRunnableClass.class.getName(), "1000");

    String reqBody = GSON.toJson(request);
    List<Callable<HttpResponse>> calls = new ArrayList<>();
    int concurrentRequests = 2;

    for (int i = 0; i < concurrentRequests; i++) {
      calls.add(
        () -> {
          HttpResponse response = HttpRequests.execute(
            HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
              .withBody(reqBody).build(),
            new DefaultHttpRequestConfig(false));
          return response;
        }
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
    Assert.assertTrue(taskWorkerService.state() == Service.State.TERMINATED);
  }

  public static class TestRunnableClass extends RunnableTask {
    @Override
    protected byte[] run(String param) throws Exception {
      if (!param.equals("")) {
        Thread.sleep(Integer.valueOf(param));
      }
      return param.getBytes();
    }

    @Override
    protected void startUp() throws Exception {

    }

    @Override
    protected void shutDown() throws Exception {

    }
  }
}
