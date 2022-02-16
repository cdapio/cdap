/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.system;

import com.google.common.util.concurrent.Service.State;
import com.google.gson.Gson;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.InMemoryDiscoveryService;
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


public class SystemWorkerServiceTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerServiceTest.class);
  private static final Gson GSON = new Gson();

  private SystemWorkerService systemWorkerService;
  private CompletableFuture<State> serviceCompletionFuture;

  private CConfiguration createCConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.SystemWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.SystemWorker.PORT, 0);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    cConf.setInt(Constants.SystemWorker.REQUEST_LIMIT, 5);
    return cConf;
  }

  private SConfiguration createSConf() {
    return SConfiguration.create();
  }

  @Before
  public void beforeTest() {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = createSConf();

    SystemWorkerService systemWorkerService = new SystemWorkerService(cConf, sConf, new InMemoryDiscoveryService(),
        (namespaceId, retryStrategy) -> null,
        new NoOpMetricsCollectionService());
    serviceCompletionFuture = SystemWorkerTestUtil.getServiceCompletionFuture(systemWorkerService);
    // start the service
    systemWorkerService.startAndWait();
    this.systemWorkerService = systemWorkerService;
  }

  @After
  public void afterTest() {
    if (systemWorkerService != null) {
      systemWorkerService.stopAndWait();
      systemWorkerService = null;
    }
  }




  @Test
  public void testStartAndStopWithValidRequest() throws IOException {
    InetSocketAddress addr = systemWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post valid request
    String want = "100";
    RunnableTaskRequest req =
        RunnableTaskRequest.getBuilder(SystemWorkerServiceTest.TestRunnableClass.class.getName()).withParam(want)
        .build();
    String reqBody = GSON.toJson(req);
    HttpResponse response = HttpRequests.execute(
        HttpRequest.post(uri.resolve("/v3Internal/system/run").toURL())
            .withBody(reqBody).build(),
        new DefaultHttpRequestConfig(false));
    SystemWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(want, response.getResponseBodyAsString());
  }

  @Test
  public void testStartAndStopWithInvalidRequest() throws Exception {
    InetSocketAddress addr = systemWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post invalid request
    RunnableTaskRequest noClassReq = RunnableTaskRequest.getBuilder("NoClass").build();
    String reqBody = GSON.toJson(noClassReq);
    HttpResponse response = HttpRequests.execute(
        HttpRequest.post(uri.resolve("/v3Internal/system/run").toURL())
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
  public void testValidConcurrentRequests() throws Exception {
    InetSocketAddress addr = systemWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    RunnableTaskRequest request =
        RunnableTaskRequest.getBuilder(SystemWorkerServiceTest.TestRunnableClass.class.getName()).
        withParam("1000").build();

    String reqBody = GSON.toJson(request);
    List<Callable<HttpResponse>> calls = new ArrayList<>();
    int concurrentRequests = 2;

    for (int i = 0; i < concurrentRequests; i++) {
      calls.add(
          () -> HttpRequests.execute(
              HttpRequest.post(uri.resolve("/v3Internal/system/run").toURL())
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
    SystemWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
    Assert.assertEquals(2, okResponse);
    Assert.assertEquals(concurrentRequests, okResponse + conflictResponse);
  }

  @Test
  public void testInvalidConcurrentRequests() throws Exception {
    InetSocketAddress addr = systemWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    RunnableTaskRequest request =
        RunnableTaskRequest.getBuilder(SystemWorkerServiceTest.TestRunnableClass.class.getName()).
            withParam("1000").build();

    String reqBody = GSON.toJson(request);
    List<Callable<HttpResponse>> calls = new ArrayList<>();
    int concurrentRequests = 6;

    for (int i = 0; i < concurrentRequests; i++) {
      calls.add(
          () -> HttpRequests.execute(
              HttpRequest.post(uri.resolve("/v3Internal/system/run").toURL())
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
    SystemWorkerTestUtil.waitForServiceCompletion(serviceCompletionFuture);
    Assert.assertEquals(5, okResponse);
    Assert.assertEquals(1, conflictResponse);
    Assert.assertEquals(concurrentRequests, okResponse + conflictResponse);
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
