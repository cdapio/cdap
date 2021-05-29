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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.worker.RunnableTask;
import io.cdap.cdap.common.internal.worker.RunnableTaskContext;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
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
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerServiceTest.class);
  private static final Gson GSON = new Gson();

  private CConfiguration createCConf(int port) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, port);
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

    // Post valid request
    String want = "100";
    RunnableTaskRequest req = new RunnableTaskRequest(TestRunnableClass.class.getName(), want);
    String reqBody = GSON.toJson(req);
    HttpResponse response = HttpRequests.execute(
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
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getResponseCode());
    BasicThrowable basicThrowable;
    basicThrowable = GSON.fromJson(response.getResponseBodyAsString(), BasicThrowable.class);
    Assert.assertTrue(basicThrowable.getClassName().contains("java.lang.ClassNotFoundException"));
    Assert.assertTrue(basicThrowable.getMessage().contains("NoClass"));
    Assert.assertNotEquals(basicThrowable.getStackTraces().length, 0);
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


  @Test
  public void testRunTaskWithQueryParameters() throws IOException {
    TaskWorkerService taskWorkerService = setupTaskWorkerService(10001);
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Create a tmp file and upload it to task worker which will read the content and return the content back.
    String content = "this is file content";
    File tmpFile = new File(tmpFolder.newFolder() + "/testRunTaskWithQueryParameters.txt");
    FileWriter writer = new FileWriter(tmpFile.getPath());
    writer.write(content);
    writer.close();

    // Submit task to worker.
    String className = FileReaderRunnableTask.class.getName();
    String param = String.valueOf(Duration.ofMillis(1).toMillis());
    String path = String.format("%s/worker/task?className=%s&param=%s",
                                Constants.Gateway.INTERNAL_API_VERSION_3,
                                className, param);
    HttpRequest request = HttpRequest.post(uri.resolve(path).toURL())
      .withBody((ContentProvider<? extends InputStream>) (() -> new FileInputStream(tmpFile))).build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(content, response.getResponseBodyAsString());

    waitForTaskWorkerToFinish(taskWorkerService);
    Assert.assertTrue(taskWorkerService.state() == Service.State.TERMINATED);
  }

  public static class TestRunnableClass implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      if (!context.getParam().equals("")) {
        Thread.sleep(Integer.valueOf(context.getParam()));
      }
      context.writeResult(context.getParam().getBytes());
    }
  }

  public static class FileReaderRunnableTask implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      URI uri = context.getFileURI();
      String content = new String(Files.readAllBytes(new File(uri).toPath()));
      context.writeResult(content.getBytes());
    }
  }
}
