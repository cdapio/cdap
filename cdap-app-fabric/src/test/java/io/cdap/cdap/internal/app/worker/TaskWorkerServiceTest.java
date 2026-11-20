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
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.TaskWorker;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.internal.remote.RemoteTaskExecutor;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizer;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerService;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.InternalAccessEnforcer;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuditLogRequest;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit test for {@link TaskWorkerService}.
 */
public class TaskWorkerServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();
  private static final MetricsCollectionService metricsCollectionService = new NoOpMetricsCollectionService();
  private static final AeadCipher aeadCipher = new NoOpAeadCipher();

  private TaskWorkerService taskWorkerService;
  private CompletableFuture<Service.State> serviceCompletionFuture;

  private SecurityManager securityManager;

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
  public void beforeTest() throws Exception {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = createSConf();

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService taskWorkerService = new TaskWorkerService(
        cConf, sConf, discoveryService, discoveryService,
        metricsCollectionService,
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher));
    serviceCompletionFuture = TaskWorkerTestUtil.getServiceCompletionFuture(
        taskWorkerService);
    // start the service
    taskWorkerService.startAndWait();
    this.taskWorkerService = taskWorkerService;
    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
  }

  @After
  public void afterTest() {
    if (taskWorkerService != null) {
      taskWorkerService.stopAndWait();
      taskWorkerService = null;
    }
    System.setSecurityManager(securityManager);
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
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher));
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
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher));
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
            new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher));
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
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher));
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
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher));
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
        new CommonNettyHttpServiceFactory(cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher));
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

  @Test(timeout = 30000)
  public void testTokenEndpointRejectedInStrictMode() throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.TaskWorker.METADATA_SERVICE_END_POINT, "http://127.0.0.1:11021/token");
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService strictWorker = new TaskWorkerService(
        cConf, createSConf(), discoveryService, discoveryService, metricsCollectionService,
        new CommonNettyHttpServiceFactory(
            cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher));
    strictWorker.startAndWait();
    try {
      InetSocketAddress addr = strictWorker.getBindAddress();
      URI uri = URI.create(String.format("http://%s:%d", addr.getHostName(), addr.getPort()));
      HttpResponse response = HttpRequests.execute(
          HttpRequest.get(uri.resolve("/v3Internal/worker/token").toURL()).build(),
          new DefaultHttpRequestConfig(false));
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, response.getResponseCode());
    } finally {
      strictWorker.stopAndWait();
    }
  }

  @Test(timeout = 30000)
  public void testCallerValidationAndRemoteTaskExecutorAuth() throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, 0);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);

    Injector secInjector = Guice.createInjector(
        new IOModule(),
        new ConfigModule(cConf),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getMasterModule());
    TokenManager tokenManager = secInjector.getInstance(TokenManager.class);
    tokenManager.startAndWait();
    InternalAccessEnforcer internalAccessEnforcer =
        secInjector.getInstance(InternalAccessEnforcer.class);
    AuthenticationContext authContext = secInjector.getInstance(AuthenticationContext.class);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    RemoteClientFactory remoteClientFactory =
        new RemoteClientFactory(discoveryService, new DefaultInternalAuthenticator(authContext));

    CommonNettyHttpServiceFactory nettyFactory = new CommonNettyHttpServiceFactory(
        cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher);
    ArtifactLocalizerService sidecar = new ArtifactLocalizerService(
        cConf,
        new ArtifactLocalizer(cConf, remoteClientFactory, (ns, retry) -> createNoOpArtifactMgr()),
        nettyFactory, remoteClientFactory, new NoOpRemoteAuthenticator(), internalAccessEnforcer);
    sidecar.startAndWait();
    cConf.setInt(Constants.ArtifactLocalizer.PORT, sidecar.getPort());

    TaskWorkerService securedWorker = new TaskWorkerService(
        cConf, createSConf(), discoveryService, discoveryService,
        metricsCollectionService, nettyFactory);
    CompletableFuture<Service.State> completionFuture =
        TaskWorkerTestUtil.getServiceCompletionFuture(securedWorker);
    securedWorker.startAndWait();

    try {
      InetSocketAddress addr = securedWorker.getBindAddress();
      URI uri = URI.create(String.format("http://%s:%d", addr.getHostName(), addr.getPort()));
      RunnableTaskRequest req = RunnableTaskRequest.getBuilder(TestRunnableClass.class.getName())
          .withParam("10")
          .withNamespace("testNamespace")
          .build();
      String reqBody = GSON.toJson(req);

      // 1. Missing X-CDAP-Internal-Authorization -> 401 without incrementing runningRequestCount.
      HttpResponse unauthResp = HttpRequests.execute(
          HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
              .withBody(reqBody)
              .build(),
          new DefaultHttpRequestConfig(false));
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, unauthResp.getResponseCode());
      Assert.assertTrue(securedWorker.isRunning());

      // 2. Forged X-CDAP-Internal-Authorization -> 403 without incrementing runningRequestCount.
      HttpResponse forgedResp = HttpRequests.execute(
          HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
              .addHeader(Constants.Security.Headers.INTERNAL_AUTHORIZATION,
                  "CDAP-Internal Zm9yZ2VkLXRva2Vu")
              .withBody(reqBody)
              .build(),
          new DefaultHttpRequestConfig(false));
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, forgedResp.getResponseCode());
      Assert.assertTrue(securedWorker.isRunning());

      // 3. RemoteTaskExecutor mints valid X-CDAP-Internal-Authorization, restores user context,
      // and preserves any AuditLogContext / AuditLogRequest.Builder on SecurityRequestContext.
      Credential userCred = new Credential("alice-oauth-token", Credential.CredentialType.EXTERNAL);
      AuditLogContext auditEntry = new AuditLogContext.Builder()
          .setAuditLoggingRequired(true)
          .setAuditLogBody("master-handler-audit-entry")
          .build();
      AuditLogRequest.Builder auditBuilder =
          new AuditLogRequest.Builder().uri("/v3/namespaces/testNamespace/apps");
      SecurityRequestContext.setUserId("alice");
      SecurityRequestContext.setUserCredential(userCred);
      SecurityRequestContext.setUserIp("10.10.0.5");
      SecurityRequestContext.enqueueAuditLogContext(auditEntry);
      SecurityRequestContext.setAuditLogRequestBuilder(auditBuilder);
      try {
        RemoteTaskExecutor executor = new RemoteTaskExecutor(
            cConf, metricsCollectionService, remoteClientFactory,
            RemoteTaskExecutor.Type.TASK_WORKER, aeadCipher);
        byte[] result = executor.runTask(req);
        Assert.assertEquals("10", new String(result, StandardCharsets.UTF_8));
        Assert.assertEquals("alice", SecurityRequestContext.getUserId());
        Assert.assertEquals(userCred, SecurityRequestContext.getUserCredential());
        Assert.assertEquals("10.10.0.5", SecurityRequestContext.getUserIp());
        Assert.assertEquals(1, SecurityRequestContext.getAuditLogQueue().size());
        Assert.assertEquals(auditEntry, SecurityRequestContext.getAuditLogQueue().peek());
        Assert.assertSame(auditBuilder, SecurityRequestContext.getAuditLogRequestBuilder());
      } finally {
        SecurityRequestContext.reset();
      }

      TaskWorkerTestUtil.waitForServiceCompletion(completionFuture);
      Assert.assertEquals(Service.State.TERMINATED, securedWorker.state());
    } finally {
      if (securedWorker.isRunning()) {
        securedWorker.stopAndWait();
      }
      sidecar.stopAndWait();
      tokenManager.stopAndWait();
    }
  }

  @Test(timeout = 30000)
  public void testClearContextFailureSchedulesRestartWithoutDoubleResponse() throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), false);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    cConf.setBoolean(TaskWorker.USER_CODE_ISOLATION_ENABLED, false);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 10);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, 0);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    RemoteClientFactory remoteClientFactory =
        new RemoteClientFactory(discoveryService, headerSetter -> {});
    CommonNettyHttpServiceFactory nettyFactory = new CommonNettyHttpServiceFactory(
        cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher);
    ArtifactLocalizerService sidecar = new ArtifactLocalizerService(
        cConf,
        new ArtifactLocalizer(cConf, remoteClientFactory, (ns, retry) -> createNoOpArtifactMgr()),
        nettyFactory, remoteClientFactory, new NoOpRemoteAuthenticator(), null);
    sidecar.startAndWait();
    cConf.setInt(Constants.ArtifactLocalizer.PORT, sidecar.getPort());

    TaskWorkerService worker = new TaskWorkerService(
        cConf, createSConf(), discoveryService, discoveryService,
        metricsCollectionService, nettyFactory);
    CompletableFuture<Service.State> completionFuture =
        TaskWorkerTestUtil.getServiceCompletionFuture(worker);
    worker.startAndWait();

    try {
      StopSidecarRunnable.sidecarToStop = sidecar;
      InetSocketAddress addr = worker.getBindAddress();
      URI uri = URI.create(String.format("http://%s:%d", addr.getHostName(), addr.getPort()));
      RunnableTaskRequest req = RunnableTaskRequest.getBuilder(StopSidecarRunnable.class.getName())
          .withParam("ok-after-clear-failure")
          .withNamespace("testNamespace")
          .build();
      HttpResponse response = HttpRequests.execute(
          HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
              .withBody(GSON.toJson(req))
              .build(),
          new DefaultHttpRequestConfig(false));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
      Assert.assertEquals("ok-after-clear-failure", response.getResponseBodyAsString());

      // Even though USER_CODE_ISOLATION_ENABLED=false, clear failure set mustRestart=true!
      TaskWorkerTestUtil.waitForServiceCompletion(completionFuture);
      Assert.assertEquals(Service.State.TERMINATED, worker.state());
    } finally {
      StopSidecarRunnable.sidecarToStop = null;
      if (worker.isRunning()) {
        worker.stopAndWait();
      }
      if (sidecar.isRunning()) {
        sidecar.stopAndWait();
      }
    }
  }

  @Test(timeout = 30000)
  public void testNsaClampsConcurrencyToOneAndRunsCleanupBeforeMetadataUnlock() throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), false);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    // Explicitly disable user code isolation and set REQUEST_LIMIT > 1: NSA mode must still clamp
    // concurrentRequestLimit to 1 so concurrent tasks get retryable 429 instead of 500.
    cConf.setBoolean(TaskWorker.USER_CODE_ISOLATION_ENABLED, false);
    cConf.setInt(TaskWorker.REQUEST_LIMIT, 4);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, 0);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    RemoteClientFactory remoteClientFactory =
        new RemoteClientFactory(discoveryService, headerSetter -> {});
    CommonNettyHttpServiceFactory nettyFactory = new CommonNettyHttpServiceFactory(
        cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher);
    ArtifactLocalizerService sidecar = new ArtifactLocalizerService(
        cConf,
        new ArtifactLocalizer(cConf, remoteClientFactory, (ns, retry) -> createNoOpArtifactMgr()),
        nettyFactory, remoteClientFactory, new NoOpRemoteAuthenticator(), null);
    sidecar.startAndWait();
    cConf.setInt(Constants.ArtifactLocalizer.PORT, sidecar.getPort());

    TaskWorkerService worker = new TaskWorkerService(
        cConf, createSConf(), discoveryService, discoveryService,
        metricsCollectionService, nettyFactory);
    worker.startAndWait();

    try {
      CleanupWhileLockedRunnable.sidecarPort = sidecar.getPort();
      CleanupWhileLockedRunnable.cleanupCallCount.set(0);
      CleanupWhileLockedRunnable.cleanupSawLockedSidecar.set(0);

      InetSocketAddress addr = worker.getBindAddress();
      URI uri = URI.create(String.format("http://%s:%d", addr.getHostName(), addr.getPort()));
      RunnableTaskRequest req = RunnableTaskRequest.getBuilder(
              CleanupWhileLockedRunnable.class.getName())
          .withParam("600")
          .withNamespace("ns_concurrent")
          .build();
      String reqBody = GSON.toJson(req);

      List<Callable<HttpResponse>> calls = new ArrayList<>();
      calls.add(() -> HttpRequests.execute(
          HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
              .withBody(reqBody)
              .build(),
          new DefaultHttpRequestConfig(false)));
      calls.add(() -> HttpRequests.execute(
          HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
              .withBody(reqBody)
              .build(),
          new DefaultHttpRequestConfig(false)));

      List<Future<HttpResponse>> responses = Executors.newFixedThreadPool(2).invokeAll(calls);
      int okCount = 0;
      int tooManyCount = 0;
      for (Future<HttpResponse> fut : responses) {
        int code = fut.get().getResponseCode();
        if (code == HttpResponseStatus.OK.code()) {
          okCount++;
        } else if (code == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
          tooManyCount++;
        }
      }
      Assert.assertEquals(1, okCount);
      Assert.assertEquals(1, tooManyCount);
      // Cleanup ran exactly once (idempotent across RunnableTaskBodyProducer.finished()) and ran
      // while the sidecar metadata context was still locked (PUT /set-context returned 403).
      Assert.assertEquals(1, CleanupWhileLockedRunnable.cleanupCallCount.get());
      Assert.assertEquals(1, CleanupWhileLockedRunnable.cleanupSawLockedSidecar.get());
    } finally {
      CleanupWhileLockedRunnable.sidecarPort = 0;
      worker.stopAndWait();
      sidecar.stopAndWait();
    }
  }

  @Test(timeout = 30000)
  public void testStrictModeFailsClosedWhenInternalAuthDisabled() throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.ENABLED, false);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), false);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, 0);

    Injector secInjector = Guice.createInjector(
        new IOModule(),
        new ConfigModule(cConf),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getMasterModule());
    InternalAccessEnforcer enforcer = secInjector.getInstance(InternalAccessEnforcer.class);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    RemoteClientFactory remoteClientFactory =
        new RemoteClientFactory(discoveryService, headerSetter -> {});
    CommonNettyHttpServiceFactory nettyFactory = new CommonNettyHttpServiceFactory(
        cConf, metricsCollectionService, auditLogContexts -> {}, aeadCipher);
    ArtifactLocalizerService sidecar = new ArtifactLocalizerService(
        cConf,
        new ArtifactLocalizer(cConf, remoteClientFactory, (ns, retry) -> createNoOpArtifactMgr()),
        nettyFactory, remoteClientFactory, new NoOpRemoteAuthenticator(), enforcer);
    sidecar.startAndWait();
    cConf.setInt(Constants.ArtifactLocalizer.PORT, sidecar.getPort());

    TaskWorkerService worker = new TaskWorkerService(
        cConf, createSConf(), discoveryService, discoveryService,
        metricsCollectionService, nettyFactory);
    worker.startAndWait();
    try {
      InetSocketAddress addr = worker.getBindAddress();
      URI uri = URI.create(String.format("http://%s:%d", addr.getHostName(), addr.getPort()));
      RunnableTaskRequest req = RunnableTaskRequest.getBuilder(TestRunnableClass.class.getName())
          .withParam("10")
          .withNamespace("testNamespace")
          .build();
      HttpResponse resp = HttpRequests.execute(
          HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
              .addHeader(Constants.Security.Headers.INTERNAL_AUTHORIZATION,
                  "CDAP-Internal Zm9yZ2VkLXRva2Vu")
              .withBody(GSON.toJson(req))
              .build(),
          new DefaultHttpRequestConfig(false));
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, resp.getResponseCode());
    } finally {
      worker.stopAndWait();
      sidecar.stopAndWait();
    }
  }

  @Test
  public void testTaskWorkerServiceLauncherStrictFlagAloneDisablesLogPublisherAndWorkerSecret()
      throws Exception {
    CConfiguration cConf = createCConf();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED, true);
    cConf.setBoolean(Constants.Twill.Security.WORKER_MOUNT_SECRET, true);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, 11021);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), false);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);

    AtomicReference<CConfiguration> localizedConfRef = new AtomicReference<>();
    TwillRunner twillRunner = Mockito.mock(TwillRunner.class);
    SecureTwillPreparer preparer = Mockito.mock(
        SecureTwillPreparer.class, Mockito.RETURNS_SELF);
    TwillController twillController = Mockito.mock(TwillController.class);
    Mockito.when(twillRunner.lookup(TaskWorkerTwillApplication.NAME))
        .thenReturn(Collections.emptyList());
    Mockito.when(twillRunner.prepare(Mockito.any(TwillApplication.class))).thenAnswer(inv -> {
      TwillApplication app = inv.getArgument(0);
      TwillSpecification spec = app.configure();
      LocalFile cConfFile = spec.getRunnables()
          .get(TaskWorkerTwillRunnable.class.getSimpleName())
          .getLocalFiles().stream()
          .filter(f -> "cConf.xml".equals(f.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Missing localized cConf.xml"));
      localizedConfRef.set(CConfiguration.create(new File(cConfFile.getURI()).toURI().toURL()));
      return preparer;
    });
    Mockito.when(preparer.start(Mockito.anyLong(), Mockito.any())).thenReturn(twillController);

    TaskWorkerServiceLauncher launcher =
        new TaskWorkerServiceLauncher(cConf, new Configuration(), twillRunner);
    launcher.run();

    CConfiguration localizedConf = localizedConfRef.get();
    Assert.assertNotNull(localizedConf);
    Assert.assertFalse(localizedConf.getBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED));
    Assert.assertTrue(cConf.getBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> envCaptor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(preparer).withEnv(
        Mockito.eq(TaskWorkerTwillRunnable.class.getSimpleName()), envCaptor.capture());
    Assert.assertEquals(
        InetAddress.getLoopbackAddress().getHostName() + ":11021",
        envCaptor.getValue().get(TaskWorker.GCE_METADATA_HOST_ENV_VAR));
    Mockito.verify(preparer)
        .withNamespacedWorkloadIdentity(TaskWorkerTwillRunnable.class.getSimpleName());
    Mockito.verify(preparer).withSecretDisk(
        Mockito.eq(ArtifactLocalizerTwillRunnable.class.getSimpleName()),
        Mockito.any(SecretDisk.class));
    Mockito.verify(preparer, Mockito.never()).withSecretDisk(
        Mockito.eq(TaskWorkerTwillRunnable.class.getSimpleName()),
        Mockito.any(SecretDisk.class));
  }

  private static ArtifactManager createNoOpArtifactMgr() {
    return new ArtifactManager() {
      @Override
      public List<ArtifactInfo> listArtifacts() {
        return Collections.emptyList();
      }

      @Override
      public List<ArtifactInfo> listArtifacts(String namespace) {
        return Collections.emptyList();
      }

      @Override
      public CloseableClassLoader createClassLoader(ArtifactInfo artifactInfo,
          @Nullable ClassLoader parentClassLoader) {
        return null;
      }

      @Override
      public CloseableClassLoader createClassLoader(String namespace, ArtifactInfo artifactInfo,
          @Nullable ClassLoader parentClassLoader) {
        return null;
      }
    };
  }

  public static class StopSidecarRunnable implements RunnableTask {
    static volatile ArtifactLocalizerService sidecarToStop;

    @Override
    public void run(RunnableTaskContext context) throws Exception {
      ArtifactLocalizerService service = sidecarToStop;
      if (service != null && service.isRunning()) {
        service.stopAndWait();
      }
      context.writeResult(context.getParam().getBytes(StandardCharsets.UTF_8));
    }
  }

  public static class CleanupWhileLockedRunnable implements RunnableTask {
    static volatile int sidecarPort;
    static final AtomicInteger cleanupCallCount = new AtomicInteger();
    static final AtomicInteger cleanupSawLockedSidecar = new AtomicInteger();

    @Override
    public void run(RunnableTaskContext context) throws Exception {
      context.setCleanupTask(() -> {
        cleanupCallCount.incrementAndGet();
        try {
          URL setUrl = new URL(String.format("http://%s:%d/set-context",
              InetAddress.getLoopbackAddress().getHostName(), sidecarPort));
          HttpResponse resp = HttpRequests.execute(HttpRequest.put(setUrl)
              .withBody(GSON.toJson(
                  new GcpMetadataTaskContext("ns_probe", "probe", "127.0.0.1", null)))
              .build());
          if (resp.getResponseCode() == 403) {
            cleanupSawLockedSidecar.incrementAndGet();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      Thread.sleep(Integer.parseInt(context.getParam()));
      context.writeResult(context.getParam().getBytes(StandardCharsets.UTF_8));
    }
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

  public class NoExitSecurityManager extends SecurityManager {

    @Override
    public void checkExit(int status) {
      throw new SecurityException("System.exit attempted, status=" + status);
    }

    @Override
    public void checkPermission(java.security.Permission perm) {
      // allow everything else
    }
  }
}
